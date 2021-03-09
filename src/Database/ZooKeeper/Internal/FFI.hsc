{-# LANGUAGE CPP              #-}
{-# LANGUAGE UnliftedFFITypes #-}

module Database.ZooKeeper.Internal.FFI where

import           Data.Word
import           Foreign
import           Foreign.C
import           GHC.Conc                          (PrimMVar)
import           Z.Foreign                         (BA##)

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
