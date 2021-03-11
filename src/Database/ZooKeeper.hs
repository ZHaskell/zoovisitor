module Database.ZooKeeper
  ( zookeeperResInit
  , Res.withResource

  , zooCreate
  , zooGet
  , zooSet

  , zookeeperInit
  , zookeeperClose
  ) where

import           Control.Concurrent                (forkIO, myThreadId,
                                                    newEmptyMVar, takeMVar,
                                                    threadCapability)
import           Control.Exception                 (mask_, onException)
import           Control.Monad                     (void, when, (<=<))
import           Data.Maybe                        (fromMaybe)
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

zooCreate :: HasCallStack
          => T.ZHandle -> CBytes -> Bytes -> T.AclVector -> T.CreateMode
          -> (T.StringCompletion -> IO ())
          -> IO ()
zooCreate zh path value acl (I.CreateMode mode) f =
  CBytes.withCBytesUnsafe path $ \path' ->
  Z.withPrimVectorUnsafe value $ \val' offset len -> mask_ $ do
    mvar <- newEmptyMVar
    sp <- newStablePtrPrimMVar mvar
    fp <- mallocForeignPtrBytes I.stringCompletionSize
    result <- withForeignPtr fp $ \data' -> do
      (cap, _) <- threadCapability =<< myThreadId
      void $ E.throwZooErrorIfNotOK =<< I.c_hs_zoo_acreate sp cap data' zh path' val' offset len acl mode
      takeMVar mvar `onException` forkIO (do takeMVar mvar; touchForeignPtr fp)
      I.peekStringCompletion data'
    -- check string completion return code
    void $ E.throwZooErrorIfNotOK $ I.strCompletionRetCode result
    f result

zooSet :: HasCallStack
       => T.ZHandle -> CBytes -> Bytes -> CInt
       -> (I.StatCompletion -> IO ())
       -> IO ()
zooSet zh path value version f =
  CBytes.withCBytesUnsafe path $ \path' ->
  Z.withPrimVectorUnsafe value $ \val' offset len -> mask_ $ do
    mvar <- newEmptyMVar
    sp <- newStablePtrPrimMVar mvar
    fp <- mallocForeignPtrBytes I.statCompletionSize
    result <- withForeignPtr fp $ \data' -> do
      (cap, _) <- threadCapability =<< myThreadId
      void $ E.throwZooErrorIfNotOK =<< I.c_hs_zoo_aset sp cap data' zh path' val' offset len version
      takeMVar mvar `onException` forkIO (do takeMVar mvar; touchForeignPtr fp)
      I.peekStatCompletion data'
    -- check stat completion return code
    void $ E.throwZooErrorIfNotOK $ I.statCompletionRetCode result
    f result

zooGet :: HasCallStack
       => T.ZHandle -> CBytes
       -> (T.DataCompletion -> IO ())
       -> IO ()
zooGet zh path f =
  CBytes.withCBytesUnsafe path $ \path' -> mask_ $ do
    mvar <- newEmptyMVar
    sp <- newStablePtrPrimMVar mvar
    fp <- mallocForeignPtrBytes I.dataCompletionSize
    result <- withForeignPtr fp $ \data' -> do
      (cap, _) <- threadCapability =<< myThreadId
      void $ E.throwZooErrorIfNotOK =<< I.c_hs_zoo_aget sp cap data' zh path' False
      takeMVar mvar `onException` forkIO (do takeMVar mvar; touchForeignPtr fp)
      I.peekDataCompletion data'
    -- check stat completion return code
    void $ E.throwZooErrorIfNotOK $ I.dataCompletionRetCode result
    f result

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
