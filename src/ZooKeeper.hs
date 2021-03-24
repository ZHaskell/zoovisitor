module ZooKeeper
  ( I.zooVersion
  , I.zooSetDebugLevel

  , zookeeperResInit
  , Res.withResource
  , Res.Resource

  , zooGetClientID

  , zooCreate
  , zooSet
  , zooGet
  , zooWatchGet
  , zooGetChildren
  , zooWatchGetChildren
  , zooGetChildren2
  , zooWatchGetChildren2
  , zooDelete
  , zooExists
  , zooWatchExists

  , zookeeperInit
  , zookeeperClose
  ) where

import           Control.Concurrent       (forkIO, myThreadId, newEmptyMVar,
                                           takeMVar, threadCapability)
import           Control.Exception        (mask_, onException)
import           Control.Monad            (void, when, (<=<))
import           Data.Maybe               (fromMaybe)
import           Data.Proxy               (Proxy (..))
import           Foreign.C                (CInt)
import           Foreign.ForeignPtr       (mallocForeignPtrBytes,
                                           touchForeignPtr, withForeignPtr)
import           Foreign.Ptr              (nullPtr)
import           GHC.Conc                 (newStablePtrPrimMVar)
import           GHC.Stack                (HasCallStack, callStack)
import           Z.Data.CBytes            (CBytes)
import qualified Z.Data.CBytes            as CBytes
import qualified Z.Data.Text.Print        as Text
import           Z.Data.Vector            (Bytes)
import qualified Z.Foreign                as Z
import qualified Z.IO.Resource            as Res

import qualified ZooKeeper.Exception      as E
import qualified ZooKeeper.Internal.FFI   as I
import qualified ZooKeeper.Internal.Types as I
import qualified ZooKeeper.Types          as T

-------------------------------------------------------------------------------

-- | Create a resource of handle to used communicate with zookeeper.
zookeeperResInit
  :: HasCallStack
  => CBytes
  -- ^ host, comma separated host:port pairs, each corresponding to a zk
  -- server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
  -> CInt
  -- ^ timeout
  -> Maybe T.ClientID
  -- ^ The id of a previously established session that this client will be
  -- reconnecting to. Pass 'Nothing' if not reconnecting to a previous
  -- session. Clients can access the session id of an established, valid,
  -- connection by calling 'zooGetClientID'. If the session corresponding to
  -- the specified clientid has expired, or if the clientid is invalid for
  -- any reason, the returned 'T.ZHandle' will be invalid -- the 'T.ZHandle'
  -- state will indicate the reason for failure (typically 'T.ZooExpiredSession').
  -> CInt
  -- ^ flags, reserved for future use. Should be set to zero.
  -> Res.Resource T.ZHandle
zookeeperResInit host timeout mclientid flags =
  Res.initResource (zookeeperInit host timeout mclientid flags) zookeeperClose

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
zooCreate zh path m_value acl (I.CreateMode mode) =
  CBytes.withCBytesUnsafe path $ \path' ->
    case m_value of
      Just value -> Z.withPrimVectorUnsafe value $ \val' offset len -> do
        let cfun = I.c_hs_zoo_acreate zh path' val' offset len acl mode
        E.throwZooErrorIfLeft =<< I.withZKAsync csize I.peekRet I.peekData cfun
      Nothing -> do
        let cfun = I.c_hs_zoo_acreate' zh path' nullPtr 0 (-1) acl mode
        E.throwZooErrorIfLeft =<< I.withZKAsync csize I.peekRet I.peekData cfun
  where
    csize = I.csize (Proxy :: Proxy T.StringCompletion)

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
       -> Bytes
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
zooSet zh path value m_version =
  CBytes.withCBytesUnsafe path $ \path' ->
  Z.withPrimVectorUnsafe value $ \val' offset len ->
    let csize = I.csize (Proxy :: Proxy T.StatCompletion)
        cfunc = I.c_hs_zoo_aset zh path' val' offset len version
        version = fromMaybe (-1) m_version
     in E.throwZooErrorIfLeft =<< I.withZKAsync csize I.peekRet I.peekData cfunc

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
  let csize = I.csize (Proxy :: Proxy T.DataCompletion)
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
  --
  -- A watch will be set at the server to notify the client if the node changes.
  -> (T.DataCompletion -> IO ())
  -- ^ The result callback when the request completes.
  --
  -- One of the following exceptions will be thrown if:
  --
  -- * ZNONODE the node does not exist.
  -- * ZNOAUTH the client does not have permission.
  -> IO ()
zooWatchGet zh path watchfn datafn = CBytes.withCBytesUnsafe path $ \path' ->
  let csize = I.csize (Proxy :: Proxy T.DataCompletion)
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
  let csize = I.csize (Proxy :: Proxy T.VoidCompletion)
      cfunc = I.c_hs_zoo_adelete zh path' version
      version = fromMaybe (-1) m_version
   in void $ E.throwZooErrorIfLeft =<< I.withZKAsync csize I.peekRet I.peekData cfunc

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
    let csize = I.csize (Proxy :: Proxy T.StatCompletion)
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
  -> (Maybe T.StatCompletion -> IO ())
  -- ^ The result callback when the request completes. Nothing means
  -- the node does not exist.
  --
  -- One of the following exceptions will be thrown if error happens:
  --
  -- * ZNOAUTH the client does not have permission.
  -> IO ()
zooWatchExists zh path watchfn statfn =
  let csize = I.csize (Proxy :: Proxy T.StatCompletion)
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
  let csize = I.csize (Proxy :: Proxy T.StringsCompletion)
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
  -> (T.StringsCompletion -> IO ())
  -- ^ The result callback when the request completes.
  --
  -- One of the following exceptions will be thrown if error happens:
  --
  -- * ZNONODE the node does not exist.
  -- * ZNOAUTH the client does not have permission.
  -> IO ()
zooWatchGetChildren zh path watchfn stringsfn =
  let csize = I.csize (Proxy :: Proxy T.StringsCompletion)
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
  let csize = I.csize (Proxy :: Proxy T.StringsStatCompletion)
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
  -> (T.StringsStatCompletion -> IO ())
  -- ^ The result callback when the request completes.
  --
  -- One of the following exceptions will be thrown if error happens:
  --
  -- * ZNONODE the node does not exist.
  -- * ZNOAUTH the client does not have permission.
  -> IO ()
zooWatchGetChildren2 zh path watchfn strsStatfn =
  let csize = I.csize (Proxy :: Proxy T.StringsStatCompletion)
      watchfn' = watchfn <=< E.throwZooErrorIfLeft
      stringsfn' = strsStatfn <=< E.throwZooErrorIfLeft
   in CBytes.withCBytesUnsafe path $ \path' ->
        I.withZKAsync2
          I.hsWatcherCtxSize (\_ -> return E.CZOK) I.peekHsWatcherCtx watchfn'
          csize I.peekRet I.peekData stringsfn'
          (I.c_hs_zoo_awget_children2 zh path')

-------------------------------------------------------------------------------

-- | Return the client session id, only valid if the connections
-- is currently connected (ie. last watcher state is 'T.ZooConnectedState')
zooGetClientID :: T.ZHandle -> IO T.ClientID
zooGetClientID = I.c_zoo_client_id

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
  :: HasCallStack
  => CBytes
  -- ^ host, comma separated host:port pairs, each corresponding to a zk
  -- server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
  -> CInt
  -- ^ timeout
  -> Maybe T.ClientID
  -- ^ The id of a previously established session that this client will be
  -- reconnecting to. Pass 'Nothing' if not reconnecting to a previous
  -- session. Clients can access the session id of an established, valid,
  -- connection by calling 'zooGetClientID'. If the session corresponding to
  -- the specified clientid has expired, or if the clientid is invalid for
  -- any reason, the returned 'T.ZHandle' will be invalid -- the 'T.ZHandle'
  -- state will indicate the reason for failure (typically 'T.ZooExpiredSession').
  -> CInt
  -- ^ flags, reserved for future use. Should be set to zero.
  -> IO T.ZHandle
zookeeperInit host timeout mclientid flags = do
  let clientid = fromMaybe (I.ClientID nullPtr) mclientid
  CBytes.withCBytesUnsafe host $ \host' -> mask_ $ do
    mvar <- newEmptyMVar
    sp <- newStablePtrPrimMVar mvar  -- freed by hs_try_takemvar()
    ctx <- mallocForeignPtrBytes I.hsWatcherCtxSize
    (ctxResult, zhResult) <- withForeignPtr ctx $ \ctx' -> do
      (cap, _) <- threadCapability =<< myThreadId
      zh <- I.c_hs_zookeeper_init sp cap ctx' host' timeout clientid flags
      when (zh == I.ZHandle nullPtr) $ E.getCErrNum >>= flip E.throwZooError callStack
      takeMVar mvar `onException` forkIO (do takeMVar mvar; touchForeignPtr ctx)
      ctxData <- I.peekHsWatcherCtx ctx'
      return (ctxData, zh)
    case I.watcherCtxState ctxResult of
      I.ZooConnectedState -> return zhResult
      state -> E.throwIO $ E.ZINVALIDSTATE $ E.ZooExInfo (Text.toText state) callStack

{-# INLINABLE zookeeperClose #-}
zookeeperClose :: T.ZHandle -> IO ()
zookeeperClose = void . E.throwZooErrorIfNotOK <=< I.c_zookeeper_close_safe
