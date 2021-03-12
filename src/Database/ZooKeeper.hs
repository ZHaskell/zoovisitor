module Database.ZooKeeper
  ( zookeeperResInit
  , Res.withResource

  , zooCreate
  , zooSet
  , zooGet
  , zooDelete

  , zookeeperInit
  , zookeeperClose
  ) where

import           Control.Concurrent                (forkIO, myThreadId,
                                                    newEmptyMVar, takeMVar,
                                                    threadCapability)
import           Control.Exception                 (mask_, onException)
import           Control.Monad                     (void, when, (<=<))
import           Data.Maybe                        (fromMaybe)
import           Data.Proxy                        (Proxy (..))
import           Foreign.C                         (CInt)
import           Foreign.ForeignPtr                (mallocForeignPtrBytes,
                                                    touchForeignPtr,
                                                    withForeignPtr)
import           Foreign.Ptr                       (nullPtr)
import           GHC.Conc                          (newStablePtrPrimMVar)
import           GHC.Stack                         (HasCallStack, callStack)
import           Z.Data.CBytes                     (CBytes)
import qualified Z.Data.CBytes                     as CBytes
import qualified Z.Data.Text.Print                 as Text
import           Z.Data.Vector                     (Bytes)
import qualified Z.Foreign                         as Z
import qualified Z.IO.Resource                     as Res

import qualified Database.ZooKeeper.Exception      as E
import qualified Database.ZooKeeper.Internal.FFI   as I
import qualified Database.ZooKeeper.Internal.Types as I
import qualified Database.ZooKeeper.Types          as T

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
  -- ^ clientid, the id of a previously established session that this
  -- client will be reconnecting to. Pass 0 if not reconnecting to a previous
  -- session. Clients can access the session id of an established, valid,
  -- connection by calling 'T.ClientID'. If the session corresponding to
  -- the specified clientid has expired, or if the clientid is invalid for
  -- any reason, the returned zhandle_t will be invalid -- the zhandle_t
  -- state will indicate the reason for failure (typically
  -- ZOO_EXPIRED_SESSION_STATE).
  -> CInt
  -- ^ flags, reserved for future use. Should be set to zero.
  -> Res.Resource T.ZHandle
zookeeperResInit host timeout mclientid flags =
  Res.initResource (zookeeperInit host timeout mclientid flags) zookeeperClose

-- | Create a node.
--
-- This method will create a node in ZooKeeper. A node can only be created if
-- it does not already exists. The Create Flags affect the creation of nodes.
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
          -> Bytes
          -- ^ The data to be stored in the node.
          -> T.AclVector
          -- ^ The initial ACL of the node. The ACL must not be null or empty.
          -> T.CreateMode
          -- ^ This parameter can be set to 'T.ZooPersistent' for normal create
          -- or an OR of the Create Flags
          -> (T.StringCompletion -> IO ())
          -- ^ The routine to invoke when the request completes. One of the
          -- following exceptions will be thrown if error happens:
          --
          -- * ZNONODE the parent node does not exist.
          -- * ZNODEEXISTS the node already exists
          -- * ZNOAUTH the client does not have permission.
          -- * ZNOCHILDRENFOREPHEMERALS cannot create children of ephemeral nodes.
          -> IO ()
zooCreate zh path value acl (I.CreateMode mode) f =
  CBytes.withCBytesUnsafe path $ \path' ->
  Z.withPrimVectorUnsafe value $ \val' offset len ->
    let csize = I.csize (Proxy :: Proxy T.StringCompletion)
        cfunc = I.c_hs_zoo_acreate zh path' val' offset len acl mode
     in f =<< I.withZKAsync csize I.peekRet I.peekData cfunc

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
       -> CInt
       -- The expected version of the node. The function will fail if
       -- the actual version of the node does not match the expected version.
       -- If -1 is used the version check will not take place.
       -> (T.StatCompletion -> IO ())
       -- ^ The routine to invoke when the request completes. One of the
       -- following exceptions will be thrown if error happens:
       --
       -- * ZOK operation completed successfully
       -- * ZNONODE the node does not exist.
       -- * ZNOAUTH the client does not have permission.
       -- * ZBADVERSION expected version does not match actual version.
       -> IO ()
zooSet zh path value version f =
  CBytes.withCBytesUnsafe path $ \path' ->
  Z.withPrimVectorUnsafe value $ \val' offset len ->
    let csize = I.csize (Proxy :: Proxy T.StatCompletion)
        cfunc = I.c_hs_zoo_aset zh path' val' offset len version
     in f =<< I.withZKAsync csize I.peekRet I.peekData cfunc

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
       -> (T.DataCompletion -> IO ())
       -- ^ The routine to invoke when the request completes. One of the
       -- following exceptions will be thrown if error happens:
       --
       -- * ZNONODE the node does not exist.
       -- * ZNOAUTH the client does not have permission.
       -> IO ()
zooGet zh path f =
  CBytes.withCBytesUnsafe path $ \path' ->
    let csize = I.csize (Proxy :: Proxy T.DataCompletion)
        cfunc = I.c_hs_zoo_aget zh path' False
     in f =<< I.withZKAsync csize I.peekRet I.peekData cfunc

-- | Delete a node in zookeeper.
--
-- Throw one of the following exceptions on failure:
--
-- * 'E.ZBADARGUMENTS' - invalid input parameters
-- * 'E.ZINVALIDSTATE' - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE
-- * 'E.ZMARSHALLINGERROR' - failed to marshall a request; possibly, out of memory
zooDelete :: HasCallStack
          => T.ZHandle
          -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
          -> CBytes
          -- ^ The name of the node. Expressed as a file name with slashes
          -- separating ancestors of the node.
          -> CInt
          -- ^ The expected version of the node. The function will fail
          -- if the actual version of the node does not match the expected version.
          -- If -1 is used the version check will not take place.
          -> (T.VoidCompletion -> IO ())
          -- ^ The routine to invoke when the request completes. One of the
          -- following exceptions will be thrown if error happens:
          --
          -- * ZNONODE      the node does not exist.
          -- * ZNOAUTH      the client does not have permission.
          -- * ZBADVERSION  expected version does not match actual version.
          -- * ZNOTEMPTY    children are present; node cannot be deleted.
          -> IO ()
zooDelete zh path version f =
  CBytes.withCBytesUnsafe path $ \path' ->
    let csize = I.csize (Proxy :: Proxy T.VoidCompletion)
        cfunc = I.c_hs_zoo_adelete zh path' version
     in f =<< I.withZKAsync csize I.peekRet I.peekData cfunc

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
  -- ^ clientid, the id of a previously established session that this
  -- client will be reconnecting to. Pass 0 if not reconnecting to a previous
  -- session. Clients can access the session id of an established, valid,
  -- connection by calling \ref zoo_client_id. If the session corresponding to
  -- the specified clientid has expired, or if the clientid is invalid for
  -- any reason, the returned zhandle_t will be invalid -- the zhandle_t
  -- state will indicate the reason for failure (typically
  -- ZOO_EXPIRED_SESSION_STATE).
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
