btree documentation

New Methods:

ERROR_T Insert(const KEY_T &key, const VALUE_T &value);
ERROR_T LookupLeaf(const SIZE_T &node, const KEY_T &key, vector<SIZE_T> &path);
ERROR_T Rebalance(const SIZE_T &node, vector<SIZE_T> path);

Our implementation is essentially a b+ tree. Every time we split we include
the key again in it's >= diskblock so that the key value pair is eventually stored
in a leaf node.

Our Insert function will call LookupOrUpdateInternal first to check to see if
the key to be inserted already exists, if the key does exist it will return an
error code of ERROR_INSANE. If the key does not exist then we check to see if
there are any keys already present. If there are no keys present we set our
rootNode to point to the key and we connect a leaf to the root node that
contains the key value pair that we want to insert. If there are already keys
in the tree we call our LookupLeaf function.

Our LookupLeaf function will traverse the tree until it finds the leaf node
where we can insert our new element. It will return a vector of pointers
representing the path to the leaf node that we want to insert our new element at.
After getting this list, we pop from the back of the list to get the leaf node.

We then find the position at which we need to add the new key value pair and we
place it into the correct position. Then we check to see if adding this new
element will exceed the limit for maxNumKeys (which we compute via the block
size) in a node. If adding the new element does not exceed cause the number of
keys to exceed the maximum then we move on and return ERROR_NOERROR. If
however adding the new element causes the number of keys to exceed maxNumKeys,
then we call our rebalance function to split the node.

Our rebalance algorithm splits the node in question and it uses the path of
pointers that we found earlier with LookupLeaf to recursively walk up the
parent path and guarantee the sanity of each parent node.

When we split we use the midpoint as the key on which we divide on. We move the
midpoint into the parent node but we also keep a copy of the the
midpoint in the new left node and all other keys past the midpoint in the new
right node.

We then try to add the key on which we split into the parent node and while
doing this we first check to see if the node we are splitting is the root node.
If the node we are splitting is the root then we must build a new root node.
We set the key of our new root to be our splitKey and then connect our newly
created left and right nodes to the new root. If the node we are splitting is
not the root then we simply find the parent from the path of parent nodes that
we have stored in our vector. We then insert the splitKey into its proper
place within the parent node. After inserting into the parent we then check to
see if adding the split key causes the parent node to need to be rebalanced and
recursively call rebalance if the parent node's number of keys now exceeds
maxNumKeys. Ultimately we will recurse on rebalance until we find either find
a parent where adding the split key does not cause the node to become full or
we split the root node and create a new root node, in which case there are no
more nodes to look at further and we finish rebalancing.
