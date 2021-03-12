{-# LANGUAGE CPP              #-}
{-# LANGUAGE UnliftedFFITypes #-}

module Database.ZooKeeper.Internal.FFI where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad                     (void)
import           Data.Word
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Stack                         (HasCallStack)
import           Z.Foreign                         (BA##)

import           Database.ZooKeeper.Exception
import           Database.ZooKeeper.Internal.Types

#include "hs_zk.h"

-------------------------------------------------------------------------------

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
    :: StablePtr PrimMVar -> Int -> Ptr StringCompletion
    -> ZHandle
    -> BA## Word8
    -> BA## Word8 -> Int -> Int
    -> AclVector
    -> CInt
    -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_aset"
  c_hs_zoo_aset
    :: StablePtr PrimMVar -> Int -> Ptr StatCompletion
    -> ZHandle
    -> BA## Word8
    -> BA## Word8 -> Int -> Int
    -> CInt
    -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_aget"
  c_hs_zoo_aget
    :: StablePtr PrimMVar -> Int -> Ptr DataCompletion
    -> ZHandle
    -> BA## Word8
    -> Bool
    -> IO CInt

foreign import ccall unsafe "hs_zk.h hs_zoo_adelete"
  c_hs_zoo_adelete
    :: ZHandle
    -> BA## Word8 -> CInt
    -> StablePtr PrimMVar -> Int -> Ptr VoidCompletion
    -> IO CInt

-------------------------------------------------------------------------------
-- Misc

withZKAsync :: HasCallStack
            => Int -> (Ptr a -> IO a)
            -> (StablePtr PrimMVar -> Int -> Ptr a -> IO CInt)
            -> IO a
{-# INLINE withZKAsync #-}
withZKAsync size peekResult f = mask_ $ do
  mvar <- newEmptyMVar
  sp <- newStablePtrPrimMVar mvar
  fp <- mallocForeignPtrBytes size
  withForeignPtr fp $ \data' -> do
    (cap, _) <- threadCapability =<< myThreadId
    void $ throwZooErrorIfNotOK =<< f sp cap data'
    takeMVar mvar `onException` forkIO (do takeMVar mvar; touchForeignPtr fp)
    peekResult data'
