module ZooKeeper
  ( I.zooVersion

  , zookeeperResInit
  , U.withResource
  , U.Resource

  , I.zooSetDebugLevel
  , StdLogStream (..)
  , zooSetStdLogStream
  , FileLogStream
  , withZooSetFileLogStream

  , zooCreate
  , zooCreateIfMissing
  , zooSet
  , zooGet
  , zooWatchGet
  , zooGetChildren
  , zooWatchGetChildren
  , zooGetChildren2
  , zooWatchGetChildren2
  , zooDelete
  , zooDeleteAll
  , zooExists
  , zooWatchExists
  , zooGetAcl

  , zooMulti
  , zooCreateOpInit
  , zooDeleteOpInit
  , zooSetOpInit
  , zooCheckOpInit
  , assertZooOpResultOK

  , zooClientID
  , I.peekClientId
  , zooState
  , zooRecvTimeout
  , isUnrecoverable

    -- * Internal function
  , zookeeperInit
  , zookeeperClose
  , openFileLogStream
  , closeFileLogStream
  ) where

import           Control.Exception        (bracket, catch, throwIO)
import           Control.Monad            (void, when, zipWithM, (<=<))
import           Data.Bifunctor           (first)
import           Data.Maybe               (fromMaybe)
import           Foreign.C                (CFile, CInt)
import           Foreign.Ptr              (FunPtr, Ptr, freeHaskellFunPtr,
                                           nullFunPtr, nullPtr, plusPtr)
import           GHC.Stack                (HasCallStack)
import qualified Z.Data.CBytes            as CBytes
import           Z.Data.CBytes            (CBytes)
import           Z.Data.Vector            (Bytes)
import qualified Z.Foreign                as Z

import qualified ZooKeeper.Exception      as E
import qualified ZooKeeper.Internal.FFI   as I
import qualified ZooKeeper.Internal.Types as I
import qualified ZooKeeper.Internal.Utils as U
import qualified ZooKeeper.Types          as T

-------------------------------------------------------------------------------

-- TODO: use ForeignPtr instead of resource style?

-- | Create a resource of handle to used communicate with zookeeper.
zookeeperResInit
  :: CBytes
  -- ^ host, comma separated host:port pairs, each corresponding to a zk
  -- server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
  -> Maybe T.WatcherFn
  -- ^ the global watcher callback function. When notifications are
  -- triggered this function will be invoked.
  --
  -- Note that each callback will be called in a different thread of execution.
  -> CInt
  -- ^ session expiration time in milliseconds
  -> Maybe T.ClientID
  -- ^ The id of a previously established session that this client will be
  -- reconnecting to. Pass 'Nothing' if not reconnecting to a previous
  -- session. Clients can access the session id of an established, valid,
  -- connection by calling 'zooClientID'. If the session corresponding to
  -- the specified clientid has expired, or if the clientid is invalid for
  -- any reason, the returned 'T.ZHandle' will be invalid -- the 'T.ZHandle'
  -- state will indicate the reason for failure (typically 'T.ZooExpiredSession').
  -> CInt
  -- ^ flags, reserved for future use. Should be set to zero.
  -> U.Resource T.ZHandle
zookeeperResInit host fn timeout mclientid flags = fst <$>
  U.initResource (zookeeperInit host fn timeout mclientid flags) zookeeperClose

-- | Create a node.
--
-- This method will create a node in ZooKeeper. A node can only be created if
-- it does not already exist. The Create Flags affect the creation of nodes.
-- If 'T.ZooEphemeral' flag is set, the node will automatically get removed if
-- the client session goes away. If the 'T.ZooSequence' flag is set, a unique
-- monotonically increasing sequence number is appended to the path name. The
-- sequence number is always fixed length of 10 digits, 0 padded.
--
-- Throw one of the following exceptions on failure:
--
-- * ZBADARGUMENTS - invalid input parameters
-- * ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
-- * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
zooCreate :: HasCallStack
          => T.ZHandle
          -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
          -> CBytes
          -- ^ The name of the node. Expressed as a file name with slashes
          -- separating ancestors of the node.
          -> Maybe Bytes
          -- ^ The data to be stored in the node.
          -> T.AclVector
          -- ^ The initial ACL of the node. The ACL must not be null or empty.
          -> T.CreateMode
          -- ^ This parameter can be set to 'T.ZooPersistent' for normal create
          -- or an OR of the Create Flags
          -> IO T.StringCompletion
          -- ^ The result when the request completes. One of the
          -- following exceptions will be thrown if error happens:
          --
          -- * ZNONODE the parent node does not exist.
          -- * ZNODEEXISTS the node already exists
          -- * ZNOAUTH the client does not have permission.
          -- * ZNOCHILDRENFOREPHEMERALS cannot create children of ephemeral nodes.
zooCreate zh path m_value acl mode =
  CBytes.withCBytesUnsafe path $ \path' ->
    case m_value of
      Just value -> Z.withPrimVectorUnsafe value $ \val' offset len -> do
        let cfun = I.c_hs_zoo_acreate zh path' val' offset len acl mode
        E.throwZooErrorIfLeft =<< I.withZKAsync csize I.peekRet I.peekData cfun
      Nothing -> do
        let cfun = I.c_hs_zoo_acreate' zh path' nullPtr 0 (-1) acl mode
        E.throwZooErrorIfLeft =<< I.withZKAsync csize I.peekRet I.peekData cfun
  where
    csize = I.csize @T.StringCompletion

-- TODO: Add option to create its parents too
-- see: https://hackage.haskell.org/package/directory-1.3.7.0/docs/System-Directory.html#v:createDirectoryIfMissing
zooCreateIfMissing
  :: T.ZHandle
  -> CBytes
  -> Maybe Bytes
  -> T.AclVector
  -> T.CreateMode
  -> IO (Maybe T.StringCompletion)
zooCreateIfMissing zh path m_value acl mode =
  catch (Just <$> zooCreate zh path m_value acl mode) $
    \(_ :: E.ZNODEEXISTS) -> pure Nothing

-- | Sets the data associated with a node.
--
-- Throw one of the following exceptions on failure:
--
-- * ZBADARGUMENTS - invalid input parameters
-- * ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
-- * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
zooSet :: HasCallStack
       => T.ZHandle
       -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
       -> CBytes
       -- ^ The name of the node. Expressed as a file name with slashes
       -- separating ancestors of the node.
       -> Maybe Bytes
       -- ^ Data to be written to the node.
       -> Maybe CInt
       -- ^ The expected version of the node. The function will fail
       -- if the actual version of the node does not match the expected version.
       -- If Nothing is used the version check will not take place.
       -> IO T.StatCompletion
       -- ^ The result when the request completes. One of the
       -- following exceptions will be thrown if error happens:
       --
       -- * ZOK operation completed successfully
       -- * ZNONODE the node does not exist.
       -- * ZNOAUTH the client does not have permission.
       -- * ZBADVERSION expected version does not match actual version.
zooSet zh path m_value m_version = CBytes.withCBytesUnsafe path $ \path' -> do
  let csize = I.csize @T.StatCompletion
      version = fromMaybe (-1) m_version
  case m_value of
    Just value -> Z.withPrimVectorUnsafe value $ \val' offset len -> do
      let cfunc = I.c_hs_zoo_aset zh path' val' offset len version
      E.throwZooErrorIfLeft =<< I.withZKAsync csize I.peekRet I.peekData cfunc
    Nothing -> do
      let cfunc = I.c_hs_zoo_aset' zh path' nullPtr 0 (-1) version
      E.throwZooErrorIfLeft =<< I.withZKAsync csize I.peekRet I.peekData cfunc

-- | Gets the data associated with a node.
--
-- Throw one of the following exceptions on failure:
--
-- * ZBADARGUMENTS - invalid input parameters
-- * ZINVALIDSTATE - zhandle state is either in ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
-- * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
zooGet :: HasCallStack
       => T.ZHandle
       -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
       -> CBytes
       -- ^ The name of the node. Expressed as a file name with slashes
       -- separating ancestors of the node.
       -> IO T.DataCompletion
       -- ^ The result when the request completes. One of the
       -- following exceptions will be thrown if:
       --
       -- * ZNONODE the node does not exist.
       -- * ZNOAUTH the client does not have permission.
zooGet zh path = CBytes.withCBytesUnsafe path $ \path' ->
  let csize = I.csize @T.DataCompletion
      cfunc = I.c_hs_zoo_aget zh path' 0
    in E.throwZooErrorIfLeft =<< I.withZKAsync csize I.peekRet I.peekData cfunc

-- | Gets the data associated with a node.
--
-- Throw one of the following exceptions on failure:
--
-- * ZBADARGUMENTS - invalid input parameters
-- * ZINVALIDSTATE - zhandle state is either in ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
-- * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
zooWatchGet
  :: HasCallStack
  => T.ZHandle
  -> CBytes
  -> (T.HsWatcherCtx -> IO ())
  -- ^ The watcher callback.
  -- A watch will be set at the server to notify the client if the node changes.
  --
  --  /NOTE: this works different with c client. You will only receive data
  --  and child watches, no session watches./
  -> (T.DataCompletion -> IO ())
  -- ^ The result callback when the request completes.
  --
  -- One of the following exceptions will be thrown if:
  --
  -- * ZNONODE the node does not exist.
  -- * ZNOAUTH the client does not have permission.
  -> IO ()
zooWatchGet zh path watchfn datafn = CBytes.withCBytesUnsafe path $ \path' ->
  let csize = I.csize @T.DataCompletion
      watchfn' = watchfn <=< E.throwZooErrorIfLeft
      datafn' = datafn <=< E.throwZooErrorIfLeft
   in I.withZKAsync2
        I.hsWatcherCtxSize (\_ -> return E.CZOK) I.peekHsWatcherCtx watchfn'
        csize I.peekRet I.peekData datafn'
        (I.c_hs_zoo_awget zh path')

-- | Delete a node in zookeeper.
--
-- Throw one of the following exceptions on failure:
--
-- * 'E.ZBADARGUMENTS' - invalid input parameters
-- * 'E.ZINVALIDSTATE' - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
-- * 'E.ZMARSHALLINGERROR' - failed to marshall a request; possibly, out of memory
--
-- Throw one of the following exceptions if the request completes failed:
--
-- * ZNONODE      the node does not exist.
-- * ZNOAUTH      the client does not have permission.
-- * ZBADVERSION  expected version does not match actual version.
-- * ZNOTEMPTY    children are present; node cannot be deleted.
zooDelete :: HasCallStack
          => T.ZHandle
          -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
          -> CBytes
          -- ^ The name of the node. Expressed as a file name with slashes
          -- separating ancestors of the node.
          -> Maybe CInt
          -- ^ The expected version of the node. The function will fail
          -- if the actual version of the node does not match the expected version.
          -- If Nothing is used the version check will not take place.
          -> IO ()
zooDelete zh path m_version = CBytes.withCBytesUnsafe path $ \path' ->
  let csize = I.csize @T.VoidCompletion
      cfunc = I.c_hs_zoo_adelete zh path' version
      version = fromMaybe (-1) m_version
   in void $ E.throwZooErrorIfLeft =<< I.withZKAsync csize I.peekRet I.peekData cfunc

-- | Delete a node and all its children in zookeeper.
--
-- NOTE: the path should not have a trailing '/'
--
-- If fails, this will throw exceptions, check `zooDeleteAll` and `zooGetChildren`
-- for more details.
zooDeleteAll :: HasCallStack => T.ZHandle -> CBytes -> IO ()
zooDeleteAll zh path = do
  T.StringsCompletion (T.StringVector children) <- zooGetChildren zh path
  mapM_ (zooDeleteAll zh . ((path <> "/") <>)) children
  zooDelete zh path Nothing

-- | Checks the existence of a node in zookeeper.
--
-- Throw one of the following exceptions on failure:
--
-- * ZBADARGUMENTS - invalid input parameters
-- * ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
-- * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
zooExists :: HasCallStack
          => T.ZHandle
          -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
          -> CBytes
          -- ^ The name of the node. Expressed as a file name with slashes
          -- separating ancestors of the node.
          -> IO (Maybe T.StatCompletion)
          -- ^ The result when the request completes. Nothing means
          -- the node does not exist.
          --
          -- One of the following exceptions will be thrown if error happens:
          --
          -- * ZNOAUTH the client does not have permission.
zooExists zh path =
  CBytes.withCBytesUnsafe path $ \path' ->
    let csize = I.csize @T.StatCompletion
        cfunc = I.c_hs_zoo_aexists zh path' 0
     in E.throwZooErrorIfLeft' (== E.CZNONODE) =<< I.withZKAsync csize I.peekRet I.peekData cfunc

-- | Checks the existence of a node in zookeeper.
--
-- This function is similar to 'zooExists' except it allows one specify
-- a watcher object. The function will be called once the watch has fired.
--
-- Note that the watch will fire both when the node is created and its associated
-- data is set.
--
-- Note that there is only one thread for triggering callbacks. Which means this
-- function will first block on the completion, and then wating on the watcher.
--
-- Throw one of the following exceptions on failure:
--
-- * ZBADARGUMENTS - invalid input parameters
-- * ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
-- * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
zooWatchExists
  :: HasCallStack
  => T.ZHandle
  -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
  -> CBytes
  -- ^ The name of the node. Expressed as a file name with slashes
  -- separating ancestors of the node.
  -> (T.HsWatcherCtx -> IO ())
  -- ^ The watcher callback.
  --
  -- A watch will set on the specified znode on the server. The watch will be
  -- set even if the node does not exist. This allows clients to watch for
  -- nodes to appear.
  --
  --  /NOTE: this works different with c client. You will only receive data
  --  and child watches, no session watches./
  -> (Maybe T.StatCompletion -> IO ())
  -- ^ The result callback when the request completes. Nothing means
  -- the node does not exist.
  --
  -- One of the following exceptions will be thrown if error happens:
  --
  -- * ZNOAUTH the client does not have permission.
  -> IO ()
zooWatchExists zh path watchfn statfn =
  let csize = I.csize @T.StatCompletion
      watchfn' = watchfn <=< E.throwZooErrorIfLeft
      statfn' = statfn <=< E.throwZooErrorIfLeft' (== E.CZNONODE)
   in CBytes.withCBytesUnsafe path $ \path' ->
        I.withZKAsync2
          I.hsWatcherCtxSize (\_ -> return E.CZOK) I.peekHsWatcherCtx watchfn'
          csize I.peekRet I.peekData statfn'
          (I.c_hs_zoo_awexists zh path')

-- | Lists the children of a node.
--
-- Throw one of the following exceptions on failure:
--
-- * ZBADARGUMENTS - invalid input parameters
-- * ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
-- * ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
zooGetChildren
  :: HasCallStack
  => T.ZHandle
  -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
  -> CBytes
  -- ^ The name of the node. Expressed as a file name with slashes
  -- separating ancestors of the node.
  -> IO T.StringsCompletion
  -- ^ The result when the request completes.
  --
  -- Throw one of the following exceptions if the request completes failed:
  --
  -- * ZNONODE the node does not exist.
  -- * ZNOAUTH the client does not have permission.
zooGetChildren zh path = CBytes.withCBytesUnsafe path $ \path' -> do
  let csize = I.csize @T.StringsCompletion
      cfunc = I.c_hs_zoo_aget_children zh path' 0
    in E.throwZooErrorIfLeft =<< I.withZKAsync csize I.peekRet I.peekData cfunc

-- | Lists the children of a node.
--
-- This function is similar to 'zooGetChildren' except it allows one specify
-- a watcher object.
--
-- Note that there is only one thread for triggering callbacks. Which means this
-- function will first block on the completion, and then wating on the watcher.
--
-- Throw one of the following exceptions on failure:
--
-- ZBADARGUMENTS - invalid input parameters
-- ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
-- ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
zooWatchGetChildren
  :: HasCallStack
  => T.ZHandle
  -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
  -> CBytes
  -- ^ The name of the node. Expressed as a file name with slashes
  -- separating ancestors of the node.
  -> (T.HsWatcherCtx -> IO ())
  -- ^ The watcher callback. A watch will be set at the server to notify
  --  the client if the node changes.
  --
  --  /NOTE: this works different with c client. You will only receive data
  --  and child watches, no session watches./
  -> (T.StringsCompletion -> IO ())
  -- ^ The result callback when the request completes.
  --
  -- One of the following exceptions will be thrown if error happens:
  --
  -- * ZNONODE the node does not exist.
  -- * ZNOAUTH the client does not have permission.
  -> IO ()
zooWatchGetChildren zh path watchfn stringsfn =
  let csize = I.csize @T.StringsCompletion
      watchfn' = watchfn <=< E.throwZooErrorIfLeft
      stringsfn' = stringsfn <=< E.throwZooErrorIfLeft
   in CBytes.withCBytesUnsafe path $ \path' ->
        I.withZKAsync2
          I.hsWatcherCtxSize (\_ -> return E.CZOK) I.peekHsWatcherCtx watchfn'
          csize I.peekRet I.peekData stringsfn'
          (I.c_hs_zoo_awget_children zh path')

-- | Lists the children of a node, and get the parent stat.
--
-- This function is new in version 3.3.0
--
-- Throw one of the following exceptions on failure:
--
-- * 'E.ZBADARGUMENTS' - invalid input parameters
-- * 'E.ZINVALIDSTATE' - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
-- * 'E.ZMARSHALLINGERROR' - failed to marshall a request; possibly, out of memory
zooGetChildren2
  :: HasCallStack
  => T.ZHandle
  -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
  -> CBytes
  -- ^ The name of the node. Expressed as a file name with slashes
  -- separating ancestors of the node.
  -> IO T.StringsStatCompletion
  -- ^ The result when the request completes.
  --
  -- Throw one of the following exceptions if the request completes failed:
  --
  -- * ZNONODE the node does not exist.
  -- * ZNOAUTH the client does not have permission.
zooGetChildren2 zh path = CBytes.withCBytesUnsafe path $ \path' -> do
  let csize = I.csize @T.StringsStatCompletion
      cfunc = I.c_hs_zoo_aget_children2 zh path' 0
    in E.throwZooErrorIfLeft =<< I.withZKAsync csize I.peekRet I.peekData cfunc

-- | Lists the children of a node, and get the parent stat.
--
-- This function is new in version 3.3.0
--
-- Note that there is only one thread for triggering callbacks. Which means this
-- function will first block on the completion, and then wating on the watcher.
--
-- Throw one of the following exceptions on failure:
--
-- * 'E.ZBADARGUMENTS' - invalid input parameters
-- * 'E.ZINVALIDSTATE' - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
-- * 'E.ZMARSHALLINGERROR' - failed to marshall a request; possibly, out of memory
zooWatchGetChildren2
  :: HasCallStack
  => T.ZHandle
  -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
  -> CBytes
  -- ^ The name of the node. Expressed as a file name with slashes
  -- separating ancestors of the node.
  -> (T.HsWatcherCtx -> IO ())
  -- ^ The watcher callback. A watch will be set at the server to notify
  --  the client if the node changes.
  --  /NOTE: this works different with c client. You will only receive data
  --  and child watches, no session watches./
  -> (T.StringsStatCompletion -> IO ())
  -- ^ The result callback when the request completes.
  --
  -- One of the following exceptions will be thrown if error happens:
  --
  -- * ZNONODE the node does not exist.
  -- * ZNOAUTH the client does not have permission.
  -> IO ()
zooWatchGetChildren2 zh path watchfn strsStatfn =
  let csize = I.csize @T.StringsStatCompletion
      watchfn' = watchfn <=< E.throwZooErrorIfLeft
      stringsfn' = strsStatfn <=< E.throwZooErrorIfLeft
   in CBytes.withCBytesUnsafe path $ \path' ->
        I.withZKAsync2
          I.hsWatcherCtxSize (\_ -> return E.CZOK) I.peekHsWatcherCtx watchfn'
          csize I.peekRet I.peekData stringsfn'
          (I.c_hs_zoo_awget_children2 zh path')

-- | Gets the acl associated with a node.
--
-- Throw one of the following exceptions on failure:
--
-- ZBADARGUMENTS - invalid input parameters
-- ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
-- ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory
zooGetAcl
  :: HasCallStack
  => T.ZHandle
  -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
  -> CBytes
  -- ^ The name of the node. Expressed as a file name with slashes
  -- separating ancestors of the node.
  -> IO T.AclCompletion
  -- ^ The result when the request completes.
  --
  -- Throw one of the following exceptions if the request completes failed:
  --
  -- * ZNONODE the node does not exist.
  -- * ZNOAUTH the client does not have permission.
zooGetAcl zh path = CBytes.withCBytesUnsafe path $ \path' -> do
  let csize = I.csize @T.AclCompletion
      cfunc = I.c_hs_zoo_aget_acl zh path'
   in E.throwZooErrorIfLeft =<< I.withZKAsync csize I.peekRet I.peekData cfunc

-------------------------------------------------------------------------------

-- | Atomically commits multiple zookeeper operations.
--
-- Throw exceptions if error happened, the exception will be any of the operations
-- supported by a multi op, see 'zooCreate', 'zooDelete' and 'zooSet'.
zooMulti
  :: HasCallStack
  => T.ZHandle
  -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
  -> [T.ZooOp]
  -- ^ An list of operations to commit
  -> IO [T.ZooOpResult]
zooMulti zh ops = do
  let len = length ops
      completionSize = I.csize @T.VoidCompletion
      chunkPtr ptr size = map (\i -> ptr `plusPtr` (i * size)) [0..len-1]

  mbai@(Z.MutableByteArray mbai#) <- Z.newPinnedByteArray (I.zooOpSize * len)
  mbar@(Z.MutableByteArray mbar#) <- Z.newPinnedByteArray (I.zooOpResultSize * len)
  let ptr = Z.mutableByteArrayContents mbai
      ptr_result = Z.mutableByteArrayContents mbar

  res <- mapM initOp $ zip ops (chunkPtr ptr I.zooOpSize)
  E.throwZooErrorIfLeft =<<
    I.withZKAsync' (concatMap snd res) completionSize I.peekRet I.peekData
                   (I.c_hs_zoo_amulti zh (fromIntegral len) mbai# mbar#)
  zipWithM ($) (map fst res) (chunkPtr ptr_result I.zooOpResultSize)

assertZooOpResultOK :: HasCallStack => T.ZooOpResult -> IO ()
assertZooOpResultOK (T.ZooCreateOpResult ret _) = void $ E.throwZooErrorIfNotOK ret
assertZooOpResultOK (T.ZooDeleteOpResult ret)   = void $ E.throwZooErrorIfNotOK ret
assertZooOpResultOK (T.ZooSetOpResult ret _)    = void $ E.throwZooErrorIfNotOK ret
assertZooOpResultOK (T.ZooCheckOpResult ret)    = void $ E.throwZooErrorIfNotOK ret
{-# INLINE assertZooOpResultOK #-}

-- | Internal helper function to set zoo op.
initOp :: (I.ZooOp, Ptr I.CZooOp)
       -> IO (Ptr I.CZooOpResult -> IO T.ZooOpResult, I.TouchListBytes)
-- we know that the size of this list is larger than one
initOp (I.ZooCreateOp f, p) = first I.peekZooCreateOpResult `fmap` f p
initOp (I.ZooDeleteOp f, p) = first (const I.peekZooDeleteOpResult) `fmap` f p
initOp (I.ZooSetOp    f, p) = first I.peekZooSetOpResult `fmap` f p
initOp (I.ZooCheckOp  f, p) = first (const I.peekZooCheckOpResult) `fmap` f p
{-# INLINE initOp #-}

-- | Init create op.
--
-- This function initializes a 'T.ZooOp' with the arguments for a ZOO_CREATE_OP.
zooCreateOpInit
  :: CBytes
  -- ^ The name of the node. Expressed as a file name with slashes
  -- separating ancestors of the node.
  -> Maybe Bytes
  -- ^ The data to be stored in the node.
  -> CInt
  -- ^ Max Size of the buffer results in 'ZooOpResult' which will be filled
  -- with the path of the new node (this might be different than the supplied
  -- path because of the 'T.ZooSequence' flag).
  --
  -- Note: we do NOT check if the size is non-negative.
  --
  -- If this size is 0, you will get an empty ZooCreateOpResult.
  --
  -- If the path of the new node exceeds the buffer size, the path string will
  -- be truncated to fit. The actual path of the new node in the server will
  -- not be affected by the truncation.
  -> T.AclVector
  -- ^ The initial ACL of the node. The ACL must not be null or empty.
  -> T.CreateMode
  -- ^ This parameter can be set to 'T.ZooPersistent' for normal create
  -- or an OR of the Create Flags
  -> T.ZooOp
zooCreateOpInit path m_value buflen acl mode = I.ZooCreateOp $ \op -> do
  let buflen' = buflen + 1  --  The path string will always be null-terminated.
  CBytes.withCBytesUnsafe path $ \path' -> do
    mba@(Z.MutableByteArray mba#) <- Z.newPinnedByteArray (fromIntegral buflen')
    case m_value of
        Just value -> Z.withPrimVectorUnsafe value $ \val' offset len ->
          I.c_hs_zoo_create_op_init op path' val' offset len acl mode mba# buflen'
        Nothing ->
          I.c_hs_zoo_create_op_init' op path' nullPtr 0 (-1) acl mode mba# buflen'
    mba_path <- Z.unsafeThawByteArray $ Z.ByteArray path'
    return (mba, [mba_path, mba])

-- | Init delete op.
--
-- This function initializes a 'T.ZooOp' with the arguments for a ZOO_DELETE_OP.
zooDeleteOpInit
  :: CBytes
  -- ^ The name of the node. Expressed as a file name with slashes
  -- separating ancestors of the node.
  -> Maybe CInt
  -- ^ The expected version of the node. The function will fail
  -- if the actual version of the node does not match the expected version.
  -- If Nothing is used the version check will not take place.
  -> T.ZooOp
zooDeleteOpInit path m_version = I.ZooDeleteOp $ \op -> do
  CBytes.withCBytesUnsafe path $ \path' -> do
    I.c_zoo_delete_op_init op path' (fromMaybe (-1) m_version)
    mba_path <- Z.unsafeThawByteArray $ Z.ByteArray path'
    return ((), [mba_path])

-- | Init set op.
--
-- This function initializes an 'T.ZooOp' with the arguments for a ZOO_SETDATA_OP.
zooSetOpInit
  :: CBytes
  -- ^ The name of the node. Expressed as a file name with slashes
  -- separating ancestors of the node.
  -> Maybe Bytes
  -- ^ Data to be written to the node.
  --
  --  To set NULL as data use this parameter as Nothing.
  -> Maybe CInt
  -- ^ The expected version of the node. The function will fail
  -- if the actual version of the node does not match the expected version.
  -- If Nothing is used the version check will not take place.
  -> T.ZooOp
zooSetOpInit path m_value m_version = I.ZooSetOp $ \op -> do
  CBytes.withCBytesUnsafe path $ \path' -> do
    mba@(Z.MutableByteArray mba#) <- Z.newPinnedByteArray I.statSize
    let version = fromMaybe (-1) m_version
    case m_value of
      Just value -> Z.withPrimVectorUnsafe value $ \val' offset len ->
        I.c_hs_zoo_set_op_init op path' val' offset len version mba#
      Nothing -> I.c_hs_zoo_set_op_init' op path' nullPtr 0 (-1) version mba#
    mba_path <- Z.unsafeThawByteArray $ Z.ByteArray path'
    return (mba, [mba_path, mba])

-- | Init check op.
--
-- This function initializes an 'T.ZooOp' with the arguments for a ZOO_CHECK_OP.
zooCheckOpInit
  :: CBytes
  -- ^ The name of the node. Expressed as a file name with slashes
  -- separating ancestors of the node.
  -> CInt       -- FIXME: does this can set to -1 ?
  -- ^ The expected version of the node. The function will fail
  -- if the actual version of the node does not match the expected version.
  -> T.ZooOp
zooCheckOpInit path version = I.ZooCheckOp $ \op -> do
  CBytes.withCBytesUnsafe path $ \path' -> do
    I.c_zoo_check_op_init op path' version
    mba_path <- Z.unsafeThawByteArray $ Z.ByteArray path'
    return ((), [mba_path])

-------------------------------------------------------------------------------

-- | Create a handle to used communicate with zookeeper.
--
-- This function creates a new handle and a zookeeper session that corresponds
-- to that handle. At the underlying c side, session establishment is asynchronous,
-- meaning that the session should not be considered established until (and unless)
-- an event of state ZOO_CONNECTED_STATE is received. In haskell, this will block
-- until state received.
--
-- If it fails to create a new zhandle or not connected, an exception will be
-- throwed.
zookeeperInit
  :: CBytes
  -- ^ host, comma separated host:port pairs, each corresponding to a zk
  -- server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
  -> Maybe T.WatcherFn
  -- ^ the global watcher callback function. When notifications are
  -- triggered this function will be invoked.
  --
  -- Note that each callback will be called in a different thread of execution.
  -> CInt
  -- ^ session expiration time in milliseconds
  -> Maybe T.ClientID
  -- ^ The id of a previously established session that this client will be
  -- reconnecting to. Pass 'Nothing' if not reconnecting to a previous
  -- session. Clients can access the session id of an established, valid,
  -- connection by calling 'zooClientID'. If the session corresponding to
  -- the specified clientid has expired, or if the clientid is invalid for
  -- any reason, the returned 'T.ZHandle' will be invalid -- the 'T.ZHandle'
  -- state will indicate the reason for failure (typically 'T.ZooExpiredSession').
  -> CInt
  -- ^ flags, reserved for future use. Should be set to zero.
  -> IO (T.ZHandle, FunPtr I.CWatcherFn)
zookeeperInit host mfn timeout mclientid flags = do
  let clientid = fromMaybe (I.ClientID nullPtr) mclientid
  fnPtr <- maybe (pure nullFunPtr) I.mkWatcherFnPtr mfn
  zh <- CBytes.withCBytes host $ \host' -> do
    I.zookeeper_init host' fnPtr timeout clientid nullPtr flags
  return (zh, fnPtr)

zookeeperClose :: (T.ZHandle, FunPtr I.CWatcherFn) -> IO ()
zookeeperClose (zh, fnptr) = do
  when (fnptr /= nullFunPtr) $ freeHaskellFunPtr fnptr
  void $ E.throwZooErrorIfNotOK =<< I.c_zookeeper_close zh
{-# INLINABLE zookeeperClose #-}

-- | Return the opaque client session id, only valid if the connections
-- is currently connected (ie. last watcher state is 'T.ZooConnectedState')
zooClientID :: T.ZHandle -> IO T.ClientID
zooClientID = I.c_zoo_client_id

-- | Checks if the current zookeeper connection state can't be recovered.
--
-- If True, the application must close the zhandle and then try to reconnect.
isUnrecoverable
  :: T.ZHandle
  -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
  -> IO Bool
  -- ^ Return True if connection is unrecoverable
isUnrecoverable zh = (< 0) <$> I.c_is_unrecoverable zh

-- | Get the state of the zookeeper connection
--
-- The return valud will be one of the State Consts
zooState :: T.ZHandle -> IO T.ZooState
zooState = I.c_zoo_state

-- | Return the timeout for this session, only valid if the connections
-- is currently connected (ie. last watcher state is ZOO_CONNECTED_STATE). This
-- value may change after a server re-connect.
zooRecvTimeout :: T.ZHandle -> IO CInt
zooRecvTimeout = I.c_zoo_recv_timeout

data StdLogStream = StdOutLogStream | StdErrLogStream

newtype FileLogStream = FileLogStream (Ptr CFile)

zooSetStdLogStream :: StdLogStream -> IO ()
zooSetStdLogStream StdOutLogStream = I.hs_zoo_set_std_log_stream 1
zooSetStdLogStream StdErrLogStream = I.hs_zoo_set_std_log_stream 2

-- | Sets the file to be used by the library for logging.
--
-- The file will be closed after the computation you run, and the LogStream
-- will be set to defaut stderr.
--
-- > let res = zookeeperResInit "127.0.0.1:2181" Nothing 5000 Nothing 0
-- > withZooSetFileLogStream "/tmp/zk.log" "a+" $ do
-- >   withResource res $ \zh -> do
-- >     undefined
withZooSetFileLogStream
  :: CBytes
  -- ^ File name, see 'openFileLogStream'
  -> CBytes
  -- ^ Mode, see 'openFileLogStream'
  -> IO a
  -- ^ Computation to run in-between
  -> IO a
withZooSetFileLogStream filename mode = bracket acquire release . const
  where
    acquire = do
      new@(FileLogStream new') <- openFileLogStream filename mode
      I.c_zoo_set_log_stream new'
      pure new
    -- FIXME: If zookeeper export the get LogStream api, then I can set back
    -- to the original LogStream but the stderr.
    release s = I.hs_zoo_set_std_log_stream 2 >> closeFileLogStream s

-- | Open the file by filename using the given mode.
--
-- The operation may fail with IOError if open filed.
--
-- Also see: <https://man7.org/linux/man-pages/man3/fopen.3.html>
openFileLogStream
  :: CBytes
  -- ^ File name
  -> CBytes
  -- ^ Mode
  --
  -- A string beginning with one of the following sequences:
  --
  -- r      Open text file for reading. The stream is positioned at
  --        the beginning of the file.
  --
  -- r+     Open for reading and writing. The stream is positioned at
  --        the beginning of the file.
  --
  -- w      Truncate file to zero length or create text file for
  --        writing.  The stream is positioned at the beginning of the
  --        file.
  --
  -- w+     Open for reading and writing. The file is created if it
  --        does not exist, otherwise it is truncated. The stream is
  --        positioned at the beginning of the file.
  --
  -- a      Open for appending (writing at end of file). The file is
  --        created if it does not exist. The stream is positioned at
  --        the end of the file.
  --
  -- a+     Open for reading and appending (writing at end of file).
  --        The file is created if it does not exist. Output is
  --        always appended to the end of the file.  POSIX is silent
  --        on what the initial read position is when using this mode.
  --        For glibc, the initial file position for reading is at the
  --        beginning of the file, but for Android/BSD/MacOS, the
  --        initial file position for reading is at the end of the
  --        file.
  -> IO FileLogStream
openFileLogStream filename mode =
  CBytes.withCBytesUnsafe filename $ \filename' ->
  CBytes.withCBytesUnsafe mode $ \mode' -> do
    cfilep <- I.c_fopen filename' mode'
    if (cfilep == nullPtr)
       then throwIO $ userError ("Open file " <> CBytes.unpack filename <> " failed!")
       else pure $ FileLogStream cfilep

closeFileLogStream :: FileLogStream -> IO ()
closeFileLogStream (FileLogStream p) = I.c_fflush p >> void (I.c_fclose p)
