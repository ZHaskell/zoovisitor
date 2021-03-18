{-# LANGUAGE CPP              #-}
{-# LANGUAGE UnliftedFFITypes #-}

module ZooKeeper.Internal.FFI where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad            (void)
import           Data.Version             (Version, makeVersion)
import           Data.Word
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Stack                (HasCallStack)
import           Z.Foreign                (BA##)

import           ZooKeeper.Exception
import           ZooKeeper.Internal.Types

#include "hs_zk.h"

-------------------------------------------------------------------------------

zooVersion :: Version
#ifdef ZOO_MAJOR_VERSION
zooVersion = makeVersion [ (#const ZOO_MAJOR_VERSION)
                         , (#const ZOO_MINOR_VERSION)
                         , (#const ZOO_PATCH_VERSION)
                         ]
#else
zooVersion = [0, 0, 0]  -- unsupported
#endif

foreign import ccall unsafe "hs_zk.h &logLevel"
  c_log_level :: Ptr ZooLogLevel

-- | Sets the debugging level for the zookeeper library
foreign import ccall unsafe "hs_zk.h zoo_set_debug_level"
  zooSetDebugLevel :: ZooLogLevel -> IO ()

foreign import ccall unsafe "hs_zk.h hs_zookeeper_init"
  c_hs_zookeeper_init
    :: StablePtr PrimMVar -> Int -> Ptr HsWatcherCtx
    -> BA## Word8
    -> CInt
    -> ClientID
    -> CInt
    -> IO ZHandle

foreign import ccall safe "hs_zk.h zookeeper_close"
  c_zookeeper_close_safe :: ZHandle -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_acreate"
  c_hs_zoo_acreate
    :: ZHandle
    -> BA## Word8
    -> BA## Word8 -> Int -> Int
    -> AclVector
    -> CInt
    -> StablePtr PrimMVar -> Int -> Ptr StringCompletion
    -> IO CInt
foreign import ccall unsafe "hs_zk.h hs_zoo_acreate"
  c_hs_zoo_acreate'
    :: ZHandle
    -> BA## Word8
    -> Ptr CChar -> Int -> Int
    -> AclVector
    -> CInt
    -> StablePtr PrimMVar -> Int -> Ptr StringCompletion
    -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_aset"
  c_hs_zoo_aset
    :: ZHandle
    -> BA## Word8
    -> BA## Word8 -> Int -> Int
    -> CInt
    -> StablePtr PrimMVar -> Int -> Ptr StatCompletion
    -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_aget"
  c_hs_zoo_aget
    :: ZHandle
    -> BA## Word8
    -> CInt
    -> StablePtr PrimMVar -> Int -> Ptr DataCompletion
    -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_awget"
  c_hs_zoo_awget
    :: ZHandle -> BA## Word8
    -> StablePtr PrimMVar -> StablePtr PrimMVar -> Int
    -> Ptr HsWatcherCtx -> Ptr DataCompletion
    -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_adelete"
  c_hs_zoo_adelete
    :: ZHandle
    -> BA## Word8 -> CInt
    -> StablePtr PrimMVar -> Int -> Ptr VoidCompletion
    -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_aexists"
  c_hs_zoo_aexists
    :: ZHandle -> BA## Word8 -> CInt
    -> StablePtr PrimMVar -> Int -> Ptr StatCompletion
    -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_awexists"
  c_hs_zoo_awexists
    :: ZHandle -> BA## Word8
    -> StablePtr PrimMVar -> StablePtr PrimMVar -> Int
    -> Ptr HsWatcherCtx -> Ptr StatCompletion
    -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_aget_children"
  c_hs_zoo_aget_children
    :: ZHandle -> BA## Word8 -> CInt
    -> StablePtr PrimMVar -> Int -> Ptr StringsCompletion
    -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_awget_children"
  c_hs_zoo_awget_children
    :: ZHandle -> BA## Word8
    -> StablePtr PrimMVar -> StablePtr PrimMVar -> Int
    -> Ptr HsWatcherCtx -> Ptr StringsCompletion
    -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_aget_children2"
  c_hs_zoo_aget_children2
    :: ZHandle -> BA## Word8 -> CInt
    -> StablePtr PrimMVar -> Int -> Ptr StringsStatCompletion
    -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_awget_children2"
  c_hs_zoo_awget_children2
    :: ZHandle -> BA## Word8
    -> StablePtr PrimMVar -> StablePtr PrimMVar -> Int
    -> Ptr HsWatcherCtx -> Ptr StringsStatCompletion
    -> IO CInt

-------------------------------------------------------------------------------
-- Misc

withZKAsync :: HasCallStack
            => Int -> (Ptr a -> IO CInt) -> (Ptr a -> IO a)
            -> (StablePtr PrimMVar -> Int -> Ptr a -> IO CInt)
            -> IO (Either CInt a)
{-# INLINE withZKAsync #-}
withZKAsync size peek_result peek_data f = mask_ $ do
  mvar <- newEmptyMVar
  sp <- newStablePtrPrimMVar mvar
  fp <- mallocForeignPtrBytes size
  withForeignPtr fp $ \data' -> do
    (cap, _) <- threadCapability =<< myThreadId
    void $ throwZooErrorIfNotOK =<< f sp cap data'
    takeMVar mvar `onException` forkIO (do takeMVar mvar; touchForeignPtr fp)
    rc <- peek_result data'
    case rc of
      CZOK -> Right <$> peek_data data'
      _    -> return $ Left rc

withZKAsync2
  :: HasCallStack
  => Int -> (Ptr a -> IO CInt) -> (Ptr a -> IO a)
  -> (Either CInt a -> IO ())
  -> Int -> (Ptr b -> IO CInt) -> (Ptr b -> IO b)
  -> (Either CInt b -> IO ())
  -> (StablePtr PrimMVar -> StablePtr PrimMVar -> Int -> Ptr a -> Ptr b -> IO CInt)
  -> IO ()
{-# INLINE withZKAsync2 #-}
withZKAsync2 size1 peekRet1 peekData1 f1 size2 peekRet2 peekData2 f2 g = mask_ $ do
  mvar1 <- newEmptyMVar
  sp1 <- newStablePtrPrimMVar mvar1
  fp1 <- mallocForeignPtrBytes size1

  mvar2 <- newEmptyMVar
  sp2 <- newStablePtrPrimMVar mvar2
  fp2 <- mallocForeignPtrBytes size2

  withForeignPtr fp1 $ \data1' ->
    withForeignPtr fp2 $ \data2' -> do
      (cap, _) <- threadCapability =<< myThreadId
      void $ throwZooErrorIfNotOK =<< g sp1 sp2 cap data1' data2'

      takeMVar mvar2 `onException` forkIO (do takeMVar mvar2; touchForeignPtr fp2)
      rc2 <- peekRet2 data2'
      case rc2 of
        CZOK -> f2 =<< Right <$> peekData2 data2'
        _    -> f2 $ Left rc2

      takeMVar mvar1 `onException` forkIO (do takeMVar mvar1; touchForeignPtr fp1)
      rc1 <- peekRet1 data1'
      case rc1 of
        CZOK -> f1 =<< Right <$> peekData1 data1'
        _    -> f1 $ Left rc1
