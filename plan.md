# What we need
- buffer pool (Dominik)
  - load pages
  - flush pages => make sure node is deleted, pointers removed
- create node from page (Renato)
  - serialize data from/to page
  - Create node from pageId via BufferPool whenever a node is needed

(Dominik)
- lookup key / value in node
- insert key / value into node
  - no splitting
- delete key / value into node
  - no splitting

- Presentation




# Structure of a page  

pageId, key = 8byte  
value = 10byte  

Non-leaf page:  
isleaf=0;pageId_of_parent;next_pageId;pageId_of_first_child;key;pageId_of_second_child;key;...;key;page_id_of_last_child;

Leaf page:
isleaf=1;pageId_of_parent;next;key_1;value_1;key_2;value_2;...key_n;value_n;
