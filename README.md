# Create Lake Function

This is an Azure function that listens to messages on a service bus queue and then uses these to build a data lake generation 2 with a structure defined in the initial message that was sent onto the service bus queue.

## Create Message
A JSON message needs to be sent to the service bus queue to which the function listens.

```
{
  "Path": "root16",
  "MaxDepth": 4,
  "CurrentDepth": 0,
  "NumberOfDirectories": 50,
  "NumberOfFiles": 10,
  "NumberOfAcls": 10,
  "CreateDirectories": true,
  "CreateFiles": true,
  "CreateAcls": true,
  "DirectoryPattern": "dir16-",
  "FilePattern": "file16-"
}
```
In the above message it generates a tree of directories and files in the quantities shown. Note the depth. At each directoty depth, it will send a service bus message to the same queue to process that directory. In this manner, it will recursively create a tree structure of the size and depth specified. 

## Number of files generated
If the depth is 4, then:

NumberOfFiles + NumberOfFiles*NumberOfDirectories + NumberOfFiles*NumberOfDirectories*NumberOfDirectories + NumberOfFiles*NumberOfDirectories*NumberOfDirectories*NumberOfDirectories

so for NumberOfDirectories = 50 and NumberOfFiles = 10, then the total is:

10 + (10 * 50) + (10 * 50 * 50) + (10 * 50 * 50 * 50) = 1,275,510 blobs


