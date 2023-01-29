/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _HDFS_LIBHDFS3_CLIENT_FILESYSTEM_H_
#define _HDFS_LIBHDFS3_CLIENT_FILESYSTEM_H_

#include "BlockLocation.h"
#include "DirectoryIterator.h"
#include "FileStatus.h"
#include "FileSystemStats.h"
#include "Permission.h"
#include "XmlConfig.h"
#include "server/EncryptionKey.h"
#include "server/LocatedBlocks.h"

#include <vector>

using namespace Hdfs::Internal;

namespace Hdfs {

/**
 * Use the CreateFlag as follows:
 * <ol>
 * <li> CREATE - to create a file if it does not exist,
 * else throw FileAlreadyExists.</li>
 * <li> APPEND - to append to a file if it exists,
 * else throw FileNotFoundException.</li>
 * <li> OVERWRITE - to truncate a file if it exists,
 * else throw FileNotFoundException.</li>
 * <li> CREATE|APPEND - to create a file if it does not exist,
 * else append to an existing file.</li>
 * <li> CREATE|OVERWRITE - to create a file if it does not exist,
 * else overwrite an existing file.</li>
 * <li> SyncBlock - to force closed blocks to the disk device.
 * In addition {@link OutputStream::sync()} should be called after each write,
 * if true synchronous behavior is required.</li>
 * </ol>
 *
 * Following combination is not valid and will result in
 * {@link InvalidParameter}:
 * <ol>
 * <li> APPEND|OVERWRITE</li>
 * <li> CREATE|APPEND|OVERWRITE</li>
 * </ol>
 */
enum CreateFlag {
    Create = 0x01, Overwrite = 0x02, Append = 0x04, SyncBlock = 0x08, NewBlock = 0x20
};

namespace Internal {
struct FileSystemWrapper;
}

class FileSystem {
public:

    /**
     * Construct a FileSystem
     * @param conf hdfs configuration
     */
    FileSystem(const Config & conf);

    /**
     * Copy construct of FileSystem
     */
    FileSystem(const FileSystem & other);

    /**
     * Assign operator of FileSystem
     */
    FileSystem & operator = (const FileSystem & other);

    /**
     * Destroy a HdfsFileSystem instance
     */
    ~FileSystem();

    /**
     * Connect to default hdfs.
     */
    void connect();

    /**
     * Connect to hdfs
     * @param uri hdfs connection uri, hdfs://host:port
     */
    void connect(const char * uri);

    /**
     * Connect to hdfs with user or token
     * 	username and token cannot be set at the same time
     * @param uri connection uri.
     * @param username user used to connect to hdfs
     * @param token token used to connect to hdfs
     */
    void connect(const char * uri, const char * username, const char * token);

    /**
     * disconnect from hdfs
     */
    void disconnect();

    Internal::EncryptionKey getEncryptionKeys();

    /**
     * To get default number of replication.
     * @return the default number of replication.
     */
    int getDefaultReplication() const;

    /**
     * To get the default number of block size.
     * @return the default block size.
     */
    int64_t getDefaultBlockSize() const;

    /**
     * To get the home directory.
     * @return home directory.
     */
    std::string getHomeDirectory() const;

    /**
     * To delete a file or directory.
     * @param path the path to be deleted.
     * @param recursive if path is a directory, delete the contents recursively.
     * @return return true if success.
     */
    bool deletePath(const char * path, bool recursive);

    /**
     * To create a directory which given permission.
     * @param path the directory path which is to be created.
     * @param permission directory permission.
     * @return return true if success.
     */
    bool mkdir(const char * path, const Permission & permission);

    /**
     * To create a directory which given permission.
     * If parent path does not exits, create it.
     * @param path the directory path which is to be created.
     * @param permission directory permission.
     * @return return true if success.
     */
    bool mkdirs(const char * path, const Permission & permission);

    /**
     * To get path information.
     * @param path the path which information is to be returned.
     * @return the path information.
     */
    FileStatus getFileStatus(const char * path) const;

    /**
     * Return an array containing hostnames, offset and size of
     * portions of the given file.
     *
     * This call is most helpful with DFS, where it returns
     * hostnames of machines that contain the given file.
     *
     * The FileSystem will simply return an elt containing 'localhost'.
     *
     * @param path path is used to identify an FS since an FS could have
     *          another FS that it could be delegating the call to
     * @param start offset into the given file
     * @param len length for which to get locations for
     */
    std::vector<BlockLocation> getFileBlockLocations(const char * path,
            int64_t start, int64_t len);

    /**
     * list the contents of a directory.
     * @param path The directory path.
     * @return Return a iterator to visit all elements in this directory.
     */
    DirectoryIterator listDirectory(const char * path);

    /**
     * list all the contents of a directory.
     * @param path The directory path.
     * @return Return a vector of file informations in the directory.
     */
    std::vector<FileStatus> listAllDirectoryItems(const char * path);

    /**
     * To set the owner and the group of the path.
     * username and groupname cannot be empty at the same time.
     * @param path the path which owner of group is to be changed.
     * @param username new user name.
     * @param groupname new group.
     */
    void setOwner(const char * path, const char * username,
                  const char * groupname);

    /**
     * To set the access time or modification time of a path.
     * @param path the path which access time or modification time is to be changed.
     * @param mtime new modification time.
     * @param atime new access time.
     */
    void setTimes(const char * path, int64_t mtime, int64_t atime);

    /**
     * To set the permission of a path.
     * @param path the path which permission is to be changed.
     * @param permission new permission.
     */
    void setPermission(const char * path, const Permission & permission);

    /**
     * To set the number of replication.
     * @param path the path which number of replication is to be changed.
     * @param replication new number of replication.
     * @return return true if success.
     */
    bool setReplication(const char * path, short replication);

    /**
     * To rename a path.
     * @param src old path.
     * @param dst new path.
     * @return return true if success.
     */
    bool rename(const char * src, const char * dst);

    /**
     * To set working directory.
     * @param path new working directory.
     */
    void setWorkingDirectory(const char * path);

    /**
     * To get working directory.
     * @return working directory.
     */
    std::string getWorkingDirectory() const;

    /**
     * To test if the path exist.
     * @param path the path which is to be tested.
     * @return return true if the path exist.
     */
    bool exist(const char * path) const;

    /**
     * To get the file system status.
     * @return the file system status.
     */
    FileSystemStats getStats() const;

    /**
     * Truncate the file in the indicated path to the indicated size.
     * @param src The path to the file to be truncated
     * @param size The size the file is to be truncated to
     *
     * @return true if and client does not need to wait for block recovery,
     * false if client needs to wait for block recovery.
     */
    bool truncate(const char * src, int64_t size);

    /**
     * Get a valid Delegation Token.
     *
     * @param renewer the designated renewer for the token
     * @return Token string
     * @throws IOException
     */
    std::string getDelegationToken(const char * renewer);

    /**
     * Get a valid Delegation Token using the default user as renewer.
     *
     * @return Token string
     * @throws IOException
     */
    std::string getDelegationToken();

    /**
     * Renew an existing delegation token.
     *
     * @param token delegation token obtained earlier
     * @return the new expiration time
     * @throws IOException
     */
    int64_t renewDelegationToken(const std::string & token);

    /**
     * Cancel an existing delegation token.
     *
     * @param token delegation token
     * @throws IOException
     */
    void cancelDelegationToken(const std::string & token);

    /**
     * Get locations of the blocks of the specified file within the specified range.
     * DataNode locations for each block are sorted by
     * the proximity to the client.
     *
     * The client will then have to contact
     * one of the indicated DataNodes to obtain the actual data.
     *
     * @param src file name
     * @param offset range start offset
     * @param length range length
     * @param lbs output the returned blocks
     */
    void getBlockLocations(const std::string & src, int64_t offset,
                           int64_t length, Hdfs::Internal::LocatedBlocks & lbs);

    /**
     * Append to the end of the file.
     *
     * @param src path of the file being appended.
     * @param flag create flag.
     */
    std::pair<shared_ptr<LocatedBlock>, shared_ptr<FileStatus>> append(const std::string & src, const uint32_t & flag);

    /**
     * Create a new file entry in the namespace.
     *
     * @param src path of the file being created.
     * @param masked masked permission.
     * @param flag indicates whether the file should be
     *  overwritten if it already exists or create if it does not exist or append.
     * @param createParent create missing parent directory if true
     * @param replication block replication factor.
     * @param blockSize maximum block size.
     */
    FileStatus create(const std::string & src, const Permission & masked, int flag,
                      bool createParent, short replication, int64_t blockSize);

    /**
     * To create or append a file.
     * @param path the file path.
     * @param flag creation flag, can be Create, Append or Create|Overwrite.
     * @param permission create a new file with given permission.
     * @param createParent if the parent does not exist, create it.
     * @param replication create a file with given number of replication.
     * @param blockSize  create a file with given block size.
     */
    std::pair<Internal::shared_ptr<Internal::LocatedBlock>, Internal::shared_ptr<FileStatus>>
    createOrAppend(const char * path, int flag, const Permission & permission,
                   bool createParent, int replication, int64_t blockSize);

    /**
     * Get FileSystemWrapper.
     * @return the FileSystemWrapper.
     */
    Internal::FileSystemWrapper * getFileSystemWrapper() {
        return impl;
    }

    /** 
     * Get conf.
     * @return the conf.
     */
    Config getConf() {
        return conf;
    }

private:
    Config conf;
    Internal::FileSystemWrapper * impl;

    friend class InputStream;
    friend class OutputStream;
};

}
#endif /* _HDFS_LIBHDFS3_CLIENT_FILESYSTEM_H_ */
