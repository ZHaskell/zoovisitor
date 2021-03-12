{-# LANGUAGE CPP #-}

module Database.ZooKeeper.Internal.Types where

import           Data.Int
import           Data.Proxy    (Proxy (..))
import           Foreign
import           Foreign.C
import           Z.Data.CBytes (CBytes)
import qualified Z.Data.CBytes as CBytes
import qualified Z.Data.Text   as Text
import           Z.Data.Vector (Bytes)
import qualified Z.Foreign     as Z

#include "hs_zk.h"

-------------------------------------------------------------------------------

newtype ZooLogLevel = ZooLogLevel CInt
  deriving (Show, Eq)

pattern ZooLogError, ZooLogWarn, ZooLogInfo, ZooLogDebug :: ZooLogLevel
pattern ZooLogError = ZooLogLevel (#const ZOO_LOG_LEVEL_ERROR)
pattern ZooLogWarn  = ZooLogLevel (#const ZOO_LOG_LEVEL_WARN)
pattern ZooLogInfo  = ZooLogLevel (#const ZOO_LOG_LEVEL_INFO)
pattern ZooLogDebug = ZooLogLevel (#const ZOO_LOG_LEVEL_DEBUG)

newtype ZHandle = ZHandle { unZHandle :: Ptr () }
  deriving (Show, Eq)

newtype ClientID = ClientID { unClientID :: Ptr () }
  deriving (Show, Eq)

-------------------------------------------------------------------------------

-- | ACL consts.
newtype Acl = Acl { unAcl :: CInt }
  deriving (Show, Eq)

pattern ZooPermRead :: Acl
pattern ZooPermRead = Acl (#const ZOO_PERM_READ)

pattern ZooPermWrite :: Acl
pattern ZooPermWrite = Acl (#const ZOO_PERM_WRITE)

pattern ZooPermCreate :: Acl
pattern ZooPermCreate = Acl (#const ZOO_PERM_CREATE)

pattern ZooPermDelete :: Acl
pattern ZooPermDelete = Acl (#const ZOO_PERM_DELETE)

pattern ZooPermAdmin :: Acl
pattern ZooPermAdmin = Acl (#const ZOO_PERM_ADMIN)

pattern ZooPermAll :: Acl
pattern ZooPermAll = Acl (#const ZOO_PERM_ALL)

newtype AclVector = AclVector { unAclVector :: Ptr () }
  deriving (Show, Eq)

-- | This is a completely open ACL
foreign import ccall unsafe "hs_zk.h &ZOO_OPEN_ACL_UNSAFE"
  zooOpenAclUnsafe :: AclVector

-- | This ACL gives the world the ability to read.
foreign import ccall unsafe "hs_zk.h &ZOO_READ_ACL_UNSAFE"
  zooReadAclUnsafe :: AclVector

-- | This ACL gives the creators authentication id's all permissions.
foreign import ccall unsafe "hs_zk.h &ZOO_CREATOR_ALL_ACL"
  zooCreatorAllAcl :: AclVector

-------------------------------------------------------------------------------

-- | State Consts
--
-- These constants represent the states of a zookeeper connection. They are
-- possible parameters of the watcher callback.
newtype ZooState = ZooState CInt
  deriving (Show, Eq)
  deriving newtype (Text.Print)

pattern ZooExpiredSession :: ZooState
pattern ZooExpiredSession = ZooState (#const ZOO_EXPIRED_SESSION_STATE)

pattern ZooAuthFailed :: ZooState
pattern ZooAuthFailed = ZooState (#const ZOO_AUTH_FAILED_STATE)

pattern ZooConnectingState :: ZooState
pattern ZooConnectingState = ZooState (#const ZOO_CONNECTING_STATE)

pattern ZooAssociatingState :: ZooState
pattern ZooAssociatingState = ZooState (#const ZOO_ASSOCIATING_STATE)

pattern ZooConnectedState :: ZooState
pattern ZooConnectedState = ZooState (#const ZOO_CONNECTED_STATE)

-- TODO
-- pattern ZOO_READONLY_STATE :: ZooState
-- pattern ZOO_READONLY_STATE = ZooState (#const ZOO_READONLY_STATE)
-- pattern ZOO_NOTCONNECTED_STATE :: ZooState
-- pattern ZOO_NOTCONNECTED_STATE = ZooState (#const ZOO_NOTCONNECTED_STATE)

-------------------------------------------------------------------------------

-- | These modes are used by zoo_create to affect node create.
newtype CreateMode = CreateMode { unCreateMode :: CInt }
  deriving (Show, Eq)

pattern ZooPersistent :: CreateMode
pattern ZooPersistent = CreateMode 0

-- | The znode will be deleted upon the client's disconnect.
pattern ZooEphemeral :: CreateMode
pattern ZooEphemeral = CreateMode (#const ZOO_EPHEMERAL)

pattern ZooSequence :: CreateMode
pattern ZooSequence = CreateMode (#const ZOO_SEQUENCE)

-- TODO
--pattern ZooPersistent :: CreateMode
--pattern ZooPersistent = CreateMode (#const ZOO_PERSISTENT)
--
--pattern ZooPersistentSequential :: CreateMode
--pattern ZooPersistentSequential = CreateMode (#const ZOO_PERSISTENT_SEQUENTIAL)
--
--pattern ZooEphemeralSequential :: CreateMode
--pattern ZooEphemeralSequential = CreateMode (#const ZOO_EPHEMERAL_SEQUENTIAL)
--
--pattern ZooContainer :: CreateMode
--pattern ZooContainer = CreateMode (#const ZOO_CONTAINER)
--
--pattern ZooPersistentWithTTL :: CreateMode
--pattern ZooPersistentWithTTL = CreateMode (#const ZOO_PERSISTENT_WITH_TTL)
--
--pattern ZooPersistentSequentialWithTTL :: CreateMode
--pattern ZooPersistentSequentialWithTTL = CreateMode (#const ZOO_PERSISTENT_SEQUENTIAL_WITH_TTL)

data Stat = Stat
  { statCzxid          :: Int64
  , statMzxid          :: Int64
  , statCtime          :: Int64
  , statMtime          :: Int64
  , statVersion        :: Int32
  , statCversion       :: Int32
  , statAversion       :: Int32
  , statEphemeralOwner :: Int64
  , statDataLength     :: Int32
  , statNumChildren    :: Int32
  , statPzxid          :: Int64
  } deriving (Show, Eq)

peekStat :: Ptr Stat -> IO Stat
peekStat ptr = Stat
  <$> (#peek stat_t, czxid) ptr
  <*> (#peek stat_t, mzxid) ptr
  <*> (#peek stat_t, ctime) ptr
  <*> (#peek stat_t, mtime) ptr
  <*> (#peek stat_t, version) ptr
  <*> (#peek stat_t, cversion) ptr
  <*> (#peek stat_t, aversion) ptr
  <*> (#peek stat_t, ephemeralOwner) ptr
  <*> (#peek stat_t, dataLength) ptr
  <*> (#peek stat_t, numChildren) ptr
  <*> (#peek stat_t, pzxid) ptr

-------------------------------------------------------------------------------
-- Callback datas

data HsWatcherCtx = HsWatcherCtx
  { watcherCtxZHandle :: ZHandle
  , watcherCtxType    :: CInt
  , watcherCtxState   :: ZooState
  , watcherCtxPath    :: CBytes
  } deriving Show

hsWatcherCtxSize :: Int
hsWatcherCtxSize = (#size hs_watcher_ctx_t)

peekHsWatcherCtx :: Ptr HsWatcherCtx -> IO HsWatcherCtx
peekHsWatcherCtx ptr = do
  zh_ptr <- (#peek hs_watcher_ctx_t, zh) ptr
  event_type <-(#peek hs_watcher_ctx_t, zh) ptr
  connect_state <- (#peek hs_watcher_ctx_t, state) ptr
  path_ptr <- (#peek hs_watcher_ctx_t, path) ptr
  path <- CBytes.fromCString path_ptr   -- NOT responsible for freeing value?
  return $ HsWatcherCtx (ZHandle zh_ptr) event_type (ZooState connect_state) path

class Completion a where
  {-# MINIMAL csize, peekRet, peekData #-}
  csize :: Proxy a -> Int
  peekRet :: Ptr a -> IO CInt
  peekData :: Ptr a -> IO a

newtype StringCompletion = StringCompletion { strCompletionValue :: CBytes }
  deriving Show

instance Completion StringCompletion where
  csize _ = (#size hs_string_completion_t)
  peekRet ptr = (#peek hs_string_completion_t, rc) ptr
  peekData ptr = do
    value_ptr <- (#peek hs_string_completion_t, value) ptr
    value <- CBytes.fromCString value_ptr <* free value_ptr
    return $ StringCompletion value

data DataCompletion = DataCompletion
  { dataCompletionValue   :: Bytes
  , dataCompletionStat    :: Stat
  } deriving Show

instance Completion DataCompletion where
  csize _ = (#size hs_data_completion_t)
  peekRet ptr = (#peek hs_data_completion_t, rc) ptr
  peekData ptr = do
    val_ptr <- (#peek hs_data_completion_t, value) ptr
    val_len :: CInt <- (#peek hs_data_completion_t, value_len) ptr
    val <- Z.fromPtr val_ptr (fromIntegral val_len) <* free val_ptr
    stat_ptr <- (#peek hs_data_completion_t, stat) ptr
    stat <- peekStat stat_ptr <* free stat_ptr
    return $ DataCompletion val stat

newtype StatCompletion = StatCompletion { statCompletionStat :: Stat }
  deriving Show

instance Completion StatCompletion where
  csize _ = (#size hs_stat_completion_t)
  peekRet ptr = (#peek hs_stat_completion_t, rc) ptr
  peekData ptr = do
    stat_ptr <- (#peek hs_stat_completion_t, stat) ptr
    stat <- peekStat stat_ptr <* free stat_ptr
    return $ StatCompletion stat

newtype VoidCompletion = VoidCompletion ()

instance Completion VoidCompletion where
  csize _ = (#size hs_void_completion_t)
  peekRet ptr = (#peek hs_stat_completion_t, rc) ptr
  peekData _ = return $ VoidCompletion ()

-------------------------------------------------------------------------------

-- This structure holds all the arguments necessary for one op as part
-- of a containing multi_op via 'zoo_multi' or 'zoo_amulti'.
-- This structure should be treated as opaque and initialized via
-- 'zoo_create_op_init', 'zoo_delete_op_init', 'zoo_set_op_init'
-- and 'zoo_check_op_init'.
data ZooOp

zooOpSize :: Int
zooOpSize = (#size zoo_op_t)
