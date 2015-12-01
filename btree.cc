#include <assert.h>
#include <math.h>
#include "btree.h"
#include <vector>

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) :
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize,
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique)
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
  SIZE_T blockSize = buffercache->GetBlockSize();
  maxNumKeys = (blockSize - sizeof(NodeMetadata))/(16);

}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) {
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) {
      return rc;
    }

    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) {
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) {
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;

      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock

  return superblock.Unserialize(buffercache,initblock);
}


ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}


ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) {
    return rc;
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) {
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
        // OK, so we now have the first key that's larger
    	// so we ned to recurse on the ptr immediately previous to
    	// this one, if it exists
    	rc=b.GetPtr(offset,ptr);
    	if (rc) { return rc; }
    	return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) {
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) {
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) {
      	if (op==BTREE_OP_LOOKUP) {
      	  return b.GetVal(offset,value);
      	} else {
      	  // BTREE_OP_UPDATE
      	  // WRITE ME - Done
            rc = b.SetVal(offset, value);
            if (rc) { return rc; }

            rc = b.Serialize(buffercache, node);
            if (rc) { return rc; }

      	  return ERROR_NOERROR;
      	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) {
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) {
      } else {
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) {
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) {
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) {
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) {
      if (offset==0) {
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) {
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) {
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) {
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) {
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) {
    os << "\" ]";
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME - Done

  // design b+ tree
  // on every key we split we must include key again in its >= diskblock
  // so that it is eventually included in a leaf node with its key/value pair
  // this makes deleting a key more complicated as we must get rid of all
  // instances of a key and then rebalance

  VALUE_T val;
  ERROR_T ret;

  //cout << "Starting Insert Function" << endl;

  // lookup and attempt update key
  ret = LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, val);

  switch(ret)
  {
    // key already exists, so do not do anything but update value
    case ERROR_NOERROR:
      //cout << "Key aleady in tree, value updated successfully" << endl;
      return ERROR_INSANE;
      break;
    // if key doesn't exist, we will try to insert
    case ERROR_NONEXISTENT:
    {
      // traverse to find the leaf
      // keep stack of pointers to track path down the node
      // where the key should go
      //cout << "New key, begin insert process" << endl;
      ERROR_T rc;

      BTreeNode leafNode;
      BTreeNode rootNode;
      BTreeNode rightLeafNode;

      SIZE_T leafPtr;
      SIZE_T rightLeafPtr;

      SIZE_T rootPtr = superblock.info.rootnode;
      rootNode.Unserialize(buffercache, rootPtr);


      initBlock = false;
      if (rootNode.info.numkeys != 0)
        initBlock = true;

      rootNode.Serialize(buffercache, rootPtr);

      // if no keys exist yet
      if (!initBlock)
      {
        //cout << "No keys in tree yet, adding to root" << endl;
        initBlock = true;

        // allocate new block and set the values to the first key position
        AllocateNode(leafPtr);
        leafNode = BTreeNode(BTREE_LEAF_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
        leafNode.Serialize(buffercache, leafPtr);
        rc = leafNode.Unserialize(buffercache, leafPtr);
        if (rc) { return rc; }

        leafNode.info.numkeys++;
        leafNode.SetKey(0, key);
        leafNode.SetVal(0, value);
        // serialize again after setting value and key
        leafNode.Serialize(buffercache, leafPtr);

        // connect this to the root
        rc = rootNode.Unserialize(buffercache, superblock.info.rootnode);
        if (rc) { return rc; }
        rootNode.info.numkeys++;
        rootNode.SetPtr(0, leafPtr);
        rootNode.SetKey(0, key);

        // build right node and connect
        AllocateNode(rightLeafPtr);
        rightLeafNode = BTreeNode(BTREE_LEAF_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
        rc = rightLeafNode.Serialize(buffercache, rightLeafPtr);
        if (rc) { return rc; }
        rootNode.SetPtr(1, rightLeafPtr);
        rc = rootNode.Serialize(buffercache, superblock.info.rootnode);
        if(rc) { return rc;}
      }
      // else some keys do exist so we need to find the path to the leaf node
      else
      {
        vector<SIZE_T> path;
        path.push_back(superblock.info.rootnode);

        //cout << "Starting lookupleaf" << endl;
        LookupLeaf(superblock.info.rootnode, key, path);
        // get the node from the last pointer (which will be the leaf node where we
        // we need to put our key) and remove it from stack
        //cout << "Finished lookupleaf" << endl;
        leafPtr = path.back();
        path.pop_back();

        //cout << "LeafPtr: " << leafPtr << endl;

        KEY_T testKey;
        KEY_T oldKey;
        VALUE_T oldVal;

        rc = leafNode.Unserialize(buffercache, leafPtr);
        if (rc) { return rc; }

        // increment the key count of the leaf node
        leafNode.info.numkeys++;

        // walk accross the leaf node
        if (leafNode.info.numkeys == 1)
        {
          rc = leafNode.SetKey(0, key);
          if (rc) { return rc; }
          rc = leafNode.SetVal(0, value);
          if (rc) { return rc; }
        }
        else
        {
          bool inserted = false;

          for (SIZE_T offset = 0; offset < (unsigned) leafNode.info.numkeys - 1; offset++)
          {
            rc = leafNode.GetKey(offset, testKey);
            if (rc) { return rc; }
            if (key < testKey)
            {
              // found position where our new key needs to go
              // move all other keys over by 1
              for (int offset2 = (int) leafNode.info.numkeys - 2; offset2 >= (int) offset; offset2--)
              {
                // grab old key and value
                rc = leafNode.GetKey((SIZE_T) offset2, oldKey);
                if (rc) { return rc; }
                rc = leafNode.GetVal((SIZE_T) offset2, oldVal);
                if (rc) { return rc; }
                // move old key up by 1
                rc = leafNode.SetKey((SIZE_T) offset2 + 1, oldKey);
                if (rc) { return rc; }
                rc = leafNode.SetVal((SIZE_T) offset2 + 1, oldVal);
                if (rc) { return rc; }
              }

              // found place for key so assign new key to offset
              inserted = true;
              rc = leafNode.SetKey(offset, key);
              if (rc) { return rc; }
              rc = leafNode.SetVal(offset, value);
              if (rc) { return rc; }

              break;
            }
          }

          // did not find place for key so add to the end
          if (!inserted)
          {
            rc = leafNode.SetKey(leafNode.info.numkeys - 1, key);
            if (rc) { return rc; }
            rc = leafNode.SetVal(leafNode.info.numkeys - 1, value);
            if (rc) { return rc; }
          }
        }

        // re serialize after access and write
        leafNode.Serialize(buffercache, leafPtr);

        // check if node length is over 2/3, call rebalance if so
        if ((int) leafNode.info.numkeys > (int) (2 * maxNumKeys/3)) {
          SIZE_T parentPtr = path.back();
          path.pop_back();
          rc = Rebalance(parentPtr, path);
          // if (rc) { return rc; }
        }
      }
      break;
    }
  }

  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::LookupLeaf(const SIZE_T &node, const KEY_T &key, vector<SIZE_T> &path)
{
  ERROR_T rc;
  BTreeNode b;
  SIZE_T offset;
  SIZE_T ptr;
  KEY_T testKey;

  rc = b.Unserialize(buffercache, node);
  if (rc != ERROR_NOERROR) { return rc; }

  switch (b.info.nodetype) {
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
      // scan through key/ptr pairs and recurse if possible
      for (offset = 0; offset < b.info.numkeys; offset++)
      {
        rc = b.GetKey(offset, testKey);
        if (rc) { return rc; }
        if (key < testKey)
        {
          // found first key that is larger
          // recurse on ptr immediately previous to this one
          rc = b.GetPtr(offset, ptr);
          if (rc) { return rc; }

          path.push_back(ptr);
          //cout << "path now has: " << path[0] << endl;
          return LookupLeaf(ptr, key, path);
        }
      }

      // did not find, so go to next pointer if exists
      if (b.info.numkeys > 0)
      {
        rc = b.GetPtr(b.info.numkeys, ptr);
        if (rc) { return rc; }

        path.push_back(ptr);
        return LookupLeaf(ptr, key, path);
      }
      else
      {
        // no keys at all on this node, so nowhere to go
        return ERROR_NONEXISTENT;
      }
      break;
    case BTREE_LEAF_NODE:
      path.push_back(node);
      return ERROR_NOERROR;
      break;
    default:
      return ERROR_INSANE;
      break;
  }

  return ERROR_INSANE;
}

ERROR_T BTreeIndex::Rebalance(const SIZE_T &node, vector<SIZE_T> path)
{
  ERROR_T rc;
  BTreeNode b;
  BTreeNode leftNode;
  BTreeNode rightNode;

  SIZE_T offset;

  int newType;

  rc = b.Unserialize(buffercache, node);
  if (rc) { return rc; }

  //cout << "Rebalancing" << endl;

  // allocate two new nodes
  // fill them from the place you're splitting
  SIZE_T leftPtr;
  SIZE_T rightPtr;
  AllocateNode(leftPtr);

  if (b.info.nodetype == BTREE_LEAF_NODE)
  {
    newType = BTREE_LEAF_NODE;
  }
  else
  {
    newType = BTREE_INTERIOR_NODE;
  }

  leftNode = BTreeNode(newType, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
  rc = leftNode.Serialize(buffercache, leftPtr);
  if (rc) { return rc; }
  AllocateNode(rightPtr);
  rightNode = BTreeNode(newType, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
  rc = rightNode.Serialize(buffercache, rightPtr);
  if (rc) { return rc; }
  // unserialize to write to new nodes
  rc = leftNode.Unserialize(buffercache, leftPtr);
  if (rc) { return rc; }
  rc = rightNode.Unserialize(buffercache, rightPtr);
  if (rc) { return rc; }

  // variables to hold key pos/ vals
  KEY_T keyPos;
  KEY_T testKey;
  VALUE_T val;
  SIZE_T tempPtr;

  // find splitting point
  int midpoint = (b.info.numkeys + 0.5) / 2;

  // if a leaf node
  if (b.info.nodetype == BTREE_LEAF_NODE)
  {
    // build left leaf node, include splitting key
    for (offset = 0; (int) offset < midpoint; offset++)
    {
      //cout << "Offset for building new left leaf node: " << offset << endl;
      leftNode.info.numkeys++;

      // get old node values
      rc = b.GetKey(offset, keyPos);
      if (rc) { return rc; }
      rc = b.GetVal(offset, val);
      if (rc) { return rc; }
      // set values in new left node
      rc = leftNode.SetKey(offset, keyPos);
      if (rc) { return rc; }
      rc = leftNode.SetVal(offset, val);
      if (rc) { return rc; }
    }
    // build right leaf node
    int pos = 0;
    for (offset = midpoint; offset < b.info.numkeys; offset++)
    {
      rightNode.info.numkeys++;

      //cout << "Offset for building new right leaf node: " << pos << endl;
      //cout << "Offset for total block: " << offset << endl;
      // get old node values
      rc = b.GetKey(offset, keyPos);
      if (rc) { return rc; }
      rc = b.GetVal(offset, val);
      if (rc) { return rc; }
      // set values in new right node
      rc = rightNode.SetKey(pos, keyPos);
      if (rc) { return rc; }
      rc = rightNode.SetVal(pos, val);
      if (rc) { return rc; }
      pos++;
    }
  }
  // if an interior node
  else
  {
    // build left interior node, include splitting key
    for (offset = 0; (int) offset < midpoint; offset++)
    {
      //cout << "offset for building new left interior node: " << offset << endl;
      leftNode.info.numkeys++;

      // get old node values
      rc = b.GetKey(offset, keyPos);
      if (rc) { return rc; }
      rc = b.GetPtr(offset, tempPtr);
      if (rc) { return rc; }
      // set values in new left node
      rc = leftNode.SetKey(offset, keyPos);
      if (rc) { return rc; }
      rc = leftNode.SetPtr(offset, tempPtr);
      if (rc) { return rc; }
    }
    // build right interior node
    int pos = 0;
    for (offset = midpoint; offset < b.info.numkeys; offset++)
    {
      rightNode.info.numkeys++;

      //cout << "offset for building new right interior node: " << pos << endl;
      //cout << "offset for total block: " << offset << endl;

      // get old node values
      rc = b.GetKey(offset, keyPos);
      if (rc) { return rc; }
      rc = b.GetPtr(offset, tempPtr);
      if (rc) { return rc; }
      // set values in new right node
      rc = rightNode.SetKey(pos, keyPos);
      if (rc) { return rc; }
      rc = rightNode.SetPtr(pos, tempPtr);
      if (rc) { return rc; }
      pos++;
    }

    rc = b.GetPtr(offset, tempPtr);
    if (rc) { return rc; }
    rc = rightNode.SetPtr(pos, tempPtr);
    if (rc) { return rc; }
  }

  // seriailize the new nodes
  rc = leftNode.Serialize(buffercache, leftPtr);
  if (rc) { return rc;}
  rc = rightNode.Serialize(buffercache, rightPtr);
  if (rc) { return rc;}
  rc = b.Serialize(buffercache, node);

  // find key to split on

  KEY_T splitKey;
  rc = b.GetKey(midpoint - 1, splitKey);
  if (rc) { return rc;}

  //cout << "Node type: " << b.info.nodetype << endl;

  // if we have reached the root we need to make a new root
  if (b.info.nodetype == BTREE_ROOT_NODE)
  {
    //cout << "Building new root" << endl;
    SIZE_T newRootPtr;
    BTreeNode newRootNode;
    AllocateNode(newRootPtr);

    newRootNode = BTreeNode(BTREE_ROOT_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
    superblock.info.rootnode = newRootPtr;
    newRootNode.info.rootnode = newRootPtr;
    newRootNode.info.numkeys = 1;
    newRootNode.SetKey(0, splitKey);
    newRootNode.SetPtr(0, leftPtr);
    newRootNode.SetPtr(1, rightPtr);
    rc = newRootNode.Serialize(buffercache, newRootPtr);
    if (rc) { return rc; }
  }
  // find parent node
  else
  {
    SIZE_T parentPtr = path.back();

    path.pop_back();

    BTreeNode parentNode;
    rc = parentNode.Unserialize(buffercache, parentPtr);
    if (rc) { return rc; }

    if (parentNode.info.nodetype == BTREE_SUPERBLOCK)
    {
      AllocateNode(parentPtr);
    }

    // create new parent node, increment key and copy over free list
    BTreeNode newParentNode = BTreeNode(parentNode.info.nodetype, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
    newParentNode.info.numkeys = parentNode.info.numkeys + 1;
    newParentNode.info.freelist = parentNode.info.freelist;

    bool newKeyInserted = false;
    for (offset = 0; offset < newParentNode.info.numkeys - 1; offset++)
    {
      rc = parentNode.GetKey(offset, testKey);

      if (newKeyInserted)
      {
        rc = parentNode.GetKey(offset, keyPos);
        if (rc) { return rc; }
        rc = newParentNode.SetKey(offset + 1, keyPos);
        if (rc) { return rc; }

        rc = parentNode.GetPtr(offset + 1, tempPtr);
        if (rc) { return rc; }
        rc = newParentNode.SetPtr(offset + 2, tempPtr);
        if (rc) { return rc; }

      }
      else
      {
        if (splitKey < testKey)
        {
          newKeyInserted = true;
          newParentNode.SetPtr(offset, leftPtr);
          newParentNode.SetKey(offset, splitKey);
          newParentNode.SetPtr(offset + 1, rightPtr);
          offset = offset - 1;
        }
        else
        {
          rc = parentNode.GetKey(offset, keyPos);
          if (rc) { return rc; }
          rc = newParentNode.SetKey(offset, keyPos);
          if (rc) { return rc; }

          rc = parentNode.GetPtr(offset, tempPtr);
          if (rc) { return rc; }
          rc = newParentNode.SetPtr(offset, tempPtr);
          if (rc) { return rc; }
        }
      }
    }

    if (newKeyInserted == false)
    {
      newKeyInserted = true;
      newParentNode.SetPtr(offset, leftPtr);
      newParentNode.SetKey(offset, splitKey);
      newParentNode.SetPtr(offset + 1, rightPtr);
    }

    newParentNode.Serialize(buffercache, parentPtr);

    if ((int) newParentNode.info.numkeys > (int) (2*maxNumKeys/3))
    {
      rc = Rebalance(parentPtr, path);
      if (rc) { return rc; }
    }
  }

  // deallocate the old node
  DeallocateNode(node);
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME - Done
  VALUE_T newvalue = value;
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, newvalue);
}

ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit
  //
  //
  return ERROR_UNIMPL;
}

//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) {
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);

  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) {
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) {
      for (offset=0;offset<=b.info.numkeys;offset++) {
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) {
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) {
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) {
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) {
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  // WRITE ME
  ERROR_T rc;
  rc = SanityHelper(superblock.info.rootnode);
  return rc;

}

ERROR_T BTreeIndex::SanityHelper(const SIZE_T &node) const
{

  ERROR_T rc;
  BTreeNode b;
  SIZE_T offset;
  SIZE_T tempPtr;
  KEY_T testKey;
  KEY_T tempKey;
  VALUE_T value;

  rc = b.Unserialize(buffercache, node);

  if (rc != ERROR_NOERROR) { return rc; }

  // check nodes have correct lengths
  if (b.info.numkeys > (unsigned) (2 * maxNumKeys / 3))
  {
    cout << "Current Node: " << b.info.nodetype << " has " << b.info.numkeys << " keys which is greater than 2/3 the max: " << maxNumKeys << endl;
  }

  switch (b.info.nodetype)
  {
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
    {
      for (offset = 0; offset < b.info.numkeys; offset++)
      {
        rc = b.GetKey(offset, testKey);
        if (rc) { return rc; }

        // check that keys are in proper order
        if (offset + 1 < b.info.numkeys - 1)
        {
          rc = b.GetKey(offset + 1, tempKey);
          if (tempKey < testKey)
          {
            cout << "Keys Not in Order!" << endl;
          }
        }

        rc = b.GetPtr(offset, tempPtr);
        if (rc) { return rc; }

        return SanityHelper(tempPtr);
      }

      // no problems so go to next pointer
      if (b.info.numkeys > 0)
      {
        rc = b.GetPtr(b.info.numkeys, tempPtr);
        if (rc) { return rc; }

        return SanityHelper(tempPtr);
      }
      else
      {
        cout << "Keys on this interior node do not exist" << endl;
        return ERROR_NONEXISTENT;
      }
      break;
    }
    case BTREE_LEAF_NODE:
    {
      for (offset = 0; offset < b.info.numkeys; offset++)
      {
        rc = b.GetKey(offset, testKey);
        if (rc)
        {
          cout << "leaf node is missing key" << endl;
          return rc;
        }

        rc = b.GetVal(offset, value);
        if (rc)
        {
          cout << "leaf node missing value" << endl;
          return rc;
        }

        // check if keys are not in order
        if(offset+1<b.info.numkeys){
          rc = b.GetKey(offset+1, tempKey);
          if(tempKey < testKey){
            cout<<"The keys not in order" << endl;
          }
        }
      }
      break;
    }
    default:
      return ERROR_INSANE;
      break;
  }

  return ERROR_NOERROR;
}


ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME - Done
  ERROR_T rc;
  rc = Display(os, BTREE_DEPTH_DOT);
  return os;
}
