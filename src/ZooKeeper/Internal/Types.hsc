{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE CPP                 #-}

module ZooKeeper.Internal.Types where

import           Control.Exception     (finally)
import           Control.Monad         (forM)
import           Data.ByteString.Short (ShortByteString)
import qualified Data.ByteString.Short as BShort (packCStringLen)
import           Data.Int
import           Foreign
import           Foreign.C
import           Numeric               (showHex)
import qualified Z.Data.CBytes         as CBytes
import           Z.Data.CBytes         (CBytes)
import qualified Z.Data.Text           as Text
import           Z.Data.Vector         (Bytes)
import qualified Z.Foreign             as Z

#include "hs_zk.h"

-------------------------------------------------------------------------------

type CWatcherFn = ZHandle -> CInt     -> CInt     -> CString -> Ptr () -> IO ()
type WatcherFn  = ZHandle -> ZooEvent -> ZooState -> CBytes            -> IO ()

newtype ZHandle = ZHandle { unZHandle :: Ptr () }
  deriving (Show, Eq)

-- | This structure holds the id and password for the session. This structure
-- should be treated as opaque. It is received from the server when a session
-- is established and needs to be sent back as-is when reconnecting a session.
newtype ClientID = ClientID { unClientID :: Ptr () }
  deriving (Show, Eq)

data HsClientID = HsClientID
  { clientId     :: {-# UNPACK #-} !Int64
  , clientPasswd :: {-# UNPACK #-} !ShortByteString
  } deriving (Show, Eq)

peekClientId :: ClientID -> IO HsClientID
peekClientId (ClientID ptr) = do
  client_id <- (#peek clientid_t, client_id) ptr
  -- definition in c: char passwd[16];
  let passwd_ptr = (#ptr clientid_t, passwd) ptr
      passwd_len = (#size ((clientid_t *)0)->passwd)
  -- the pointer is getting from zhandle, so here we don't need to free it
  passwd <- BShort.packCStringLen (passwd_ptr, passwd_len)
  pure $ HsClientID{clientId=client_id, clientPasswd=passwd}

newtype ZooLogLevel = ZooLogLevel CInt
  deriving (Eq, Storable)

instance Show ZooLogLevel where
  show ZooLogError = "ERROR"
  show ZooLogWarn  = "WARN"
  show ZooLogInfo  = "INFO"
  show ZooLogDebug = "DEBUG"
  show (ZooLogLevel x) = "ZooLogLevel " ++ show x

pattern ZooLogError, ZooLogWarn, ZooLogInfo, ZooLogDebug :: ZooLogLevel
pattern ZooLogError = ZooLogLevel (#const ZOO_LOG_LEVEL_ERROR)
pattern ZooLogWarn  = ZooLogLevel (#const ZOO_LOG_LEVEL_WARN)
pattern ZooLogInfo  = ZooLogLevel (#const ZOO_LOG_LEVEL_INFO)
pattern ZooLogDebug = ZooLogLevel (#const ZOO_LOG_LEVEL_DEBUG)

-- | Disable logging
pattern ZooLogSilence :: ZooLogLevel
pattern ZooLogSilence = ZooLogLevel 0

-------------------------------------------------------------------------------

-- | ACL permissions.
newtype ZooPerm = ZooPerm { unZooPerm :: CInt }
  deriving (Eq, Bits)

instance Show ZooPerm where
  show ZooPermRead   = "ZooPermRead"
  show ZooPermWrite  = "ZooPermWrite"
  show ZooPermCreate = "ZooPermCreate"
  show ZooPermDelete = "ZooPermDelete"
  show ZooPermAdmin  = "ZooPermAdmin"
  show ZooPermAll    = "ZooPermAll"
  show (ZooPerm x)   = "ZooPerm: 0x" ++ showHex x ""

pattern ZooPermRead, ZooPermWrite, ZooPermCreate, ZooPermDelete, ZooPermAdmin :: ZooPerm
pattern ZooPermRead   = ZooPerm (#const ZOO_PERM_READ)
pattern ZooPermWrite  = ZooPerm (#const ZOO_PERM_WRITE)
pattern ZooPermCreate = ZooPerm (#const ZOO_PERM_CREATE)
pattern ZooPermDelete = ZooPerm (#const ZOO_PERM_DELETE)
pattern ZooPermAdmin  = ZooPerm (#const ZOO_PERM_ADMIN)

pattern ZooPermAll :: ZooPerm
pattern ZooPermAll = ZooPerm (#const ZOO_PERM_ALL)

{-# INLINE toZooPerms #-}
toZooPerms :: CInt -> [ZooPerm]
toZooPerms n = go allPerms
  where
    go [] = []
    go (x:xs)
      | ZooPerm n .&. x == x = x : (go $! xs)
      | otherwise            = go $! xs
    allPerms = [ZooPermRead, ZooPermWrite, ZooPermCreate, ZooPermDelete, ZooPermAdmin]

{-# INLINE fromZooPerms #-}
fromZooPerms :: [ZooPerm] -> CInt
fromZooPerms = foldr (.|.) 0 . map unZooPerm

{-# INLINE compactZooPerms #-}
compactZooPerms :: [ZooPerm] -> ZooPerm
compactZooPerms = ZooPerm . fromZooPerms

data ZooAcl = ZooAcl
  { aclPerms    :: [ZooPerm]
  , aclIdScheme :: CBytes
  , aclId       :: CBytes
  } deriving (Show, Eq)

sizeOfZooAcl :: Int
sizeOfZooAcl = (#size acl_t)
{-# INLINE sizeOfZooAcl #-}

newtype AclVector = AclVector (Ptr ())
  deriving (Show, Eq)

peekAclVector :: AclVector -> IO [ZooAcl]
peekAclVector p@(AclVector ptr) = flip finally (free_acl_vector p) $ do
  count <- fromIntegral @Int32 <$> (#peek acl_vector_t, count) ptr
  data_ptr <- (#peek acl_vector_t, data) ptr
  forM [0..count-1] $ peekAclVectorIdx data_ptr

peekAclVectorIdx :: Ptr ZooAcl -> Int -> IO ZooAcl
peekAclVectorIdx ptr offset = do
  let ptr' = ptr `plusPtr` (offset * sizeOfZooAcl)
  perms <- toZooPerms <$> (#peek acl_t, perms) ptr'
  scheme_ptr <- (#peek acl_t, id.scheme) ptr'
  id_ptr <- (#peek acl_t, id.id) ptr'
  scheme <- CBytes.fromCString scheme_ptr
  acl_id <- CBytes.fromCString id_ptr
  return $ ZooAcl perms scheme acl_id

-- TODO
unsafeAllocaZooAcl :: ZooAcl -> IO Z.ByteArray
unsafeAllocaZooAcl = undefined

-- TODO
fromAclList :: [ZooAcl] -> IO AclVector
fromAclList = undefined

-- | This is a completely open ACL
foreign import ccall unsafe "hs_zk.h &ZOO_OPEN_ACL_UNSAFE"
  zooOpenAclUnsafe :: AclVector

-- | This ACL gives the world the ability to read.
foreign import ccall unsafe "hs_zk.h &ZOO_READ_ACL_UNSAFE"
  zooReadAclUnsafe :: AclVector

-- | This ACL gives the creators authentication id's all permissions.
foreign import ccall unsafe "hs_zk.h &ZOO_CREATOR_ALL_ACL"
  zooCreatorAllAcl :: AclVector

foreign import ccall unsafe "free_acl_vector"
  free_acl_vector :: AclVector -> IO ()

-------------------------------------------------------------------------------

-- | Interest Consts
--
-- These constants are used to express interest in an event and to
-- indicate to zookeeper which events have occurred. They can
-- be ORed together to express multiple interests. These flags are
-- used in the interest and event parameters of
-- zookeeper_interest and zookeeper_process.
newtype ZooInterest = ZooInterest CInt
  deriving (Eq, Storable)

instance Show ZooInterest where
  show ZookeeperWrite  = "ZookeeperWrite"
  show ZookeeperRead   = "ZookeeperRead"
  show (ZooInterest x) = "ZooInterest: " <> show x

pattern ZookeeperWrite :: ZooInterest
pattern ZookeeperWrite = ZooInterest (#const ZOOKEEPER_WRITE)

pattern ZookeeperRead :: ZooInterest
pattern ZookeeperRead = ZooInterest (#const ZOOKEEPER_READ)

-------------------------------------------------------------------------------

-- | State Consts
--
-- These constants represent the states of a zookeeper connection. They are
-- possible parameters of the watcher callback.
newtype ZooState = ZooState CInt
  deriving (Eq, Storable)
  deriving newtype (Text.Print)

instance Show ZooState where
  show ZooExpiredSession    = "ExpiredSession"
  show ZooAuthFailed        = "AuthFailed"
  show ZooConnectingState   = "ConnectingState"
  show ZooAssociatingState  = "AssociatingState"
  show ZooConnectedState    = "ConnectedState"
  show ZooReadonlyState     = "ReadonlyState"
  show ZooNotconnectedState = "NotconnectedState"
  show (ZooState x)         = "ZooState " <> show x

pattern
    ZooExpiredSession, ZooAuthFailed
  , ZooConnectingState, ZooAssociatingState, ZooConnectedState :: ZooState
pattern ZooExpiredSession   = ZooState (#const ZOO_EXPIRED_SESSION_STATE)
pattern ZooAuthFailed       = ZooState (#const ZOO_AUTH_FAILED_STATE)
pattern ZooConnectingState  = ZooState (#const ZOO_CONNECTING_STATE)
pattern ZooAssociatingState = ZooState (#const ZOO_ASSOCIATING_STATE)
pattern ZooConnectedState   = ZooState (#const ZOO_CONNECTED_STATE)

-- This a trick to determine whether the C library expose the following apis.
--
-- ZOO_VERSION was introduced by ZOOKEEPER-3635 (3.6.0-pre). Also version after
-- 3.6 exports the following states.
#ifdef ZOO_VERSION

pattern ZooReadonlyState :: ZooState
pattern ZooReadonlyState = ZooState (#const ZOO_READONLY_STATE)

pattern ZooNotconnectedState :: ZooState
pattern ZooNotconnectedState = ZooState (#const ZOO_NOTCONNECTED_STATE)

#else
-- Hardcode for zookeeper-3.4

pattern ZooReadonlyState :: ZooState
pattern ZooReadonlyState = ZooState 5

pattern ZooNotconnectedState :: ZooState
pattern ZooNotconnectedState = ZooState 999

#endif

-------------------------------------------------------------------------------

-- | Watch Types
--
-- These constants indicate the event that caused the watch event. They are
-- possible values of the first parameter of the watcher callback.
newtype ZooEvent = ZooEvent CInt
  deriving (Eq, Storable)

instance Show ZooEvent where
  show ZooCreateEvent     = "CreateEvent"
  show ZooDeleteEvent     = "DeleteEvent"
  show ZooChangedEvent    = "ChangedEvent"
  show ZooChildEvent      = "ChildEvent"
  show ZooSessionEvent    = "SessionEvent"
  show ZooNoWatchingEvent = "NoWatchingEvent"
  show (ZooEvent x)       = "ZooEvent " <> show x

-- | A node has been created.
--
-- This is only generated by watches on non-existent nodes. These watches
-- are set using 'ZooKeeper.zooWatchExists'.
pattern ZooCreateEvent :: ZooEvent
pattern ZooCreateEvent = ZooEvent (#const ZOO_CREATED_EVENT)

-- | A node has been deleted.
--
-- This is only generated by watches on nodes. These watches
-- are set using 'ZooKeeper.zooWatchExists' and 'ZooKeeper.zooWatchGet'.
pattern ZooDeleteEvent :: ZooEvent
pattern ZooDeleteEvent = ZooEvent (#const ZOO_DELETED_EVENT)

-- | A node has changed.
--
-- This is only generated by watches on nodes. These watches
-- are set using 'ZooKeeper.zooWatchExists' and 'ZooKeeper.zooWatchGet'.
pattern ZooChangedEvent :: ZooEvent
pattern ZooChangedEvent = ZooEvent (#const ZOO_CHANGED_EVENT)

-- A change as occurred in the list of children.
--
-- This is only generated by watches on the child list of a node. These watches
-- are set using 'ZooKeeper.zooWatchGetChildren' or 'ZooKeeper.zooWatchGetChildren2'.
pattern ZooChildEvent :: ZooEvent
pattern ZooChildEvent = ZooEvent (#const ZOO_CHILD_EVENT)

-- | A session has been lost.
--
-- This is generated when a client loses contact or reconnects with a server.
pattern ZooSessionEvent :: ZooEvent
pattern ZooSessionEvent = ZooEvent (#const ZOO_SESSION_EVENT)

-- | A watch has been removed.
--
-- This is generated when the server for some reason, probably a resource
-- constraint, will no longer watch a node for a client.
pattern ZooNoWatchingEvent :: ZooEvent
pattern ZooNoWatchingEvent = ZooEvent (#const ZOO_NOTWATCHING_EVENT)

-------------------------------------------------------------------------------

-- | These modes are used by zoo_create to affect node create.
newtype CreateMode = CreateMode { unCreateMode :: CInt }
  deriving (Show, Eq, Storable)

-- This a trick to determine whether the C library expose the following apis.
--
-- ZOO_VERSION was introduced by ZOOKEEPER-3635 (3.6.0-pre).
--
-- The following C constants (such as ZOO_PERSISTENT, ZOO_PERSISTENT_SEQUENTIAL
-- and ZOO_EPTHMERAL_SEQUENTIAL) are not defined on clients <= 3.4.x.
#ifdef ZOO_VERSION

pattern ZooPersistent :: CreateMode
pattern ZooPersistent = CreateMode (#const ZOO_PERSISTENT)

pattern ZooPersistentSequential :: CreateMode
pattern ZooPersistentSequential = CreateMode (#const ZOO_PERSISTENT_SEQUENTIAL)

pattern ZooEphemeralSequential :: CreateMode
pattern ZooEphemeralSequential = CreateMode (#const ZOO_EPHEMERAL_SEQUENTIAL)

pattern ZooContainer :: CreateMode
pattern ZooContainer = CreateMode (#const ZOO_CONTAINER)

pattern ZooPersistentWithTTL :: CreateMode
pattern ZooPersistentWithTTL = CreateMode (#const ZOO_PERSISTENT_WITH_TTL)

pattern ZooPersistentSequentialWithTTL :: CreateMode
pattern ZooPersistentSequentialWithTTL = CreateMode (#const ZOO_PERSISTENT_SEQUENTIAL_WITH_TTL)

#else

pattern ZooPersistent :: CreateMode
pattern ZooPersistent = CreateMode 0

pattern ZooPersistentSequential :: CreateMode
pattern ZooPersistentSequential = CreateMode 2

pattern ZooEphemeralSequential :: CreateMode
pattern ZooEphemeralSequential = CreateMode 3

pattern ZooContainer :: CreateMode
pattern ZooContainer = CreateMode 4

pattern ZooPersistentWithTTL :: CreateMode
pattern ZooPersistentWithTTL = CreateMode 5

pattern ZooPersistentSequentialWithTTL :: CreateMode
pattern ZooPersistentSequentialWithTTL = CreateMode 6

#endif

-- | The znode will be deleted upon the client's disconnect.
pattern ZooEphemeral :: CreateMode
pattern ZooEphemeral = CreateMode (#const ZOO_EPHEMERAL)

pattern ZooSequence :: CreateMode
pattern ZooSequence = CreateMode (#const ZOO_SEQUENCE)

data Stat = Stat
  { statCzxid          :: {-# UNPACK #-} !Int64
  , statMzxid          :: {-# UNPACK #-} !Int64
  , statCtime          :: {-# UNPACK #-} !Int64
  , statMtime          :: {-# UNPACK #-} !Int64
  , statVersion        :: {-# UNPACK #-} !Int32
  , statCversion       :: {-# UNPACK #-} !Int32
  , statAversion       :: {-# UNPACK #-} !Int32
  , statEphemeralOwner :: {-# UNPACK #-} !Int64
  , statDataLength     :: {-# UNPACK #-} !Int32
  , statNumChildren    :: {-# UNPACK #-} !Int32
  , statPzxid          :: {-# UNPACK #-} !Int64
  } deriving (Show, Eq)

statSize :: Int
statSize = (#size stat_t)

peekStat' :: Ptr Stat -> IO Stat
peekStat' ptr = Stat
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

peekStat :: Ptr Stat -> IO Stat
peekStat ptr = peekStat' ptr <* free ptr

newtype StringVector = StringVector { unStrVec :: [CBytes] }
  deriving Show

-- Peek a StringVector from point and then free the pointer
peekStringVector :: Ptr StringVector -> IO StringVector
peekStringVector ptr = flip finally (free_string_vector ptr) $ do
  -- NOTE: Int32 is necessary, since count is int32_t in c
  count <- fromIntegral @Int32 <$> (#peek string_vector_t, count) ptr
  StringVector <$> forM [0..count-1] (peekStringVectorIdx ptr)

peekStringVectorIdx :: Ptr StringVector -> Int -> IO CBytes
peekStringVectorIdx ptr offset = do
  ptr' <- (#peek string_vector_t, data) ptr
  data_ptr <- peek $ ptr' `plusPtr` (offset * (sizeOf ptr'))
  CBytes.fromCString data_ptr  -- this will do a copy

foreign import ccall unsafe "free_string_vector"
  free_string_vector :: Ptr StringVector -> IO ()

-------------------------------------------------------------------------------
-- Callback datas

data HsWatcherCtx = HsWatcherCtx
  { watcherCtxZHandle :: ZHandle
  , watcherCtxType    :: ZooEvent
  , watcherCtxState   :: ZooState
  , watcherCtxPath    :: Maybe CBytes
  } deriving Show

hsWatcherCtxSize :: Int
hsWatcherCtxSize = (#size hs_watcher_ctx_t)

peekHsWatcherCtx :: Ptr HsWatcherCtx -> IO HsWatcherCtx
peekHsWatcherCtx ptr = do
  zh_ptr <- (#peek hs_watcher_ctx_t, zh) ptr
  event_type <-(#peek hs_watcher_ctx_t, type) ptr
  connect_state <- (#peek hs_watcher_ctx_t, state) ptr
  path_ptr <- (#peek hs_watcher_ctx_t, path) ptr
  path <- if path_ptr == nullPtr
             then return Nothing
             else Just <$> CBytes.fromCString path_ptr <* free path_ptr
  return $ HsWatcherCtx (ZHandle zh_ptr) event_type connect_state path

class Completion a where
  {-# MINIMAL csize, peekRet, peekData #-}
  csize :: Int
  peekRet :: Ptr a -> IO CInt
  peekData :: Ptr a -> IO a

newtype StringCompletion = StringCompletion { strCompletionValue :: CBytes }
  deriving Show

instance Completion StringCompletion where
  csize = (#size hs_string_completion_t)
  peekRet ptr = (#peek hs_string_completion_t, rc) ptr
  peekData ptr = do
    value_ptr <- (#peek hs_string_completion_t, value) ptr
    value <- CBytes.fromCString value_ptr <* free value_ptr
    return $ StringCompletion value

data DataCompletion = DataCompletion
  { dataCompletionValue :: Maybe Bytes
  , dataCompletionStat  :: Stat
  } deriving (Show, Eq)

instance Completion DataCompletion where
  csize = (#size hs_data_completion_t)
  peekRet ptr = (#peek hs_data_completion_t, rc) ptr
  peekData ptr = do
    val_ptr <- (#peek hs_data_completion_t, value) ptr
    val_len :: CInt <- (#peek hs_data_completion_t, value_len) ptr
    val <- if val_len >= 0
              then Just <$> Z.fromPtr val_ptr (fromIntegral val_len) <* free val_ptr
              else return Nothing
    stat_ptr <- (#peek hs_data_completion_t, stat) ptr
    stat <- peekStat stat_ptr
    return $ DataCompletion val stat

newtype StatCompletion = StatCompletion { statCompletionStat :: Stat }
  deriving (Show, Eq)

instance Completion StatCompletion where
  csize = (#size hs_stat_completion_t)
  peekRet ptr = (#peek hs_stat_completion_t, rc) ptr
  peekData ptr = do
    stat_ptr <- (#peek hs_stat_completion_t, stat) ptr
    stat <- peekStat stat_ptr
    return $ StatCompletion stat

newtype VoidCompletion = VoidCompletion ()

instance Completion VoidCompletion where
  csize = (#size hs_void_completion_t)
  peekRet ptr = (#peek hs_stat_completion_t, rc) ptr
  peekData _ = return $ VoidCompletion ()

newtype StringsCompletion = StringsCompletion
  { strsCompletionValues :: StringVector }
  deriving Show

instance Completion StringsCompletion where
  csize = (#size hs_strings_completion_t)
  peekRet ptr = (#peek hs_strings_completion_t, rc) ptr
  peekData ptr = do
    strs_ptr <- (#peek hs_strings_completion_t, strings) ptr
    vals <- peekStringVector strs_ptr
    return $ StringsCompletion vals

data StringsStatCompletion = StringsStatCompletion
  { strsStatCompletionStrs :: StringVector
  , strsStatCompletionStat :: Stat
  } deriving Show

instance Completion StringsStatCompletion where
  csize = (#size hs_strings_stat_completion_t)
  peekRet ptr = (#peek hs_strings_stat_completion_t, rc) ptr
  peekData ptr = do
    strs_ptr <- (#peek hs_strings_stat_completion_t, strings) ptr
    vals <- peekStringVector strs_ptr
    stat_ptr <- (#peek hs_strings_stat_completion_t, stat) ptr
    stat <- peekStat stat_ptr
    return $ StringsStatCompletion vals stat

data AclCompletion = AclCompletion
  { aclCompletionAcls :: [ZooAcl]
  , aclCompletionStat :: Stat
  } deriving Show

instance Completion AclCompletion where
  csize = (#size hs_acl_completion_t)
  peekRet ptr = (#peek hs_acl_completion_t, rc) ptr
  peekData ptr = do
    acls <- peekAclVector . AclVector =<< (#peek hs_acl_completion_t, acl) ptr
    stat_ptr <- (#peek hs_acl_completion_t, stat) ptr
    stat <- peekStat stat_ptr
    return $ AclCompletion acls stat

-------------------------------------------------------------------------------

data CZooOp
data CZooOpResult

zooOpSize :: Int
zooOpSize = (#size zoo_op_t)

zooOpResultSize :: Int
zooOpResultSize = (#size zoo_op_result_t)

-- only safe on /pinned/ byte array
type ResultBytes = Z.MutableByteArray Z.RealWorld
type TouchListBytes = [Z.MutableByteArray Z.RealWorld]

-- | This structure holds all the arguments necessary for one op as part of a
-- containing multi_op via 'ZooKeeper.zooMulti'.
data ZooOp
  = ZooCreateOp (Ptr CZooOp -> IO (ResultBytes, TouchListBytes))
  | ZooDeleteOp (Ptr CZooOp -> IO ((), TouchListBytes))
  | ZooSetOp    (Ptr CZooOp -> IO (ResultBytes, TouchListBytes))
  | ZooCheckOp  (Ptr CZooOp -> IO ((), TouchListBytes))

data ZooOpResult
  = ZooCreateOpResult CInt CBytes
  | ZooDeleteOpResult CInt
  | ZooSetOpResult    CInt Stat
  | ZooCheckOpResult  CInt
  deriving (Show, Eq)

peekZooCreateOpResult :: ResultBytes -> Ptr CZooOpResult -> IO ZooOpResult
peekZooCreateOpResult (Z.MutableByteArray ba##) ptr = do
  ret <- (#peek zoo_op_result_t, err) ptr
  value <- CBytes.fromMutablePrimArray $ Z.MutablePrimArray ba##
  return $ ZooCreateOpResult ret value

peekZooDeleteOpResult :: Ptr CZooOpResult -> IO ZooOpResult
peekZooDeleteOpResult ptr = ZooDeleteOpResult <$> (#peek zoo_op_result_t, err) ptr

peekZooSetOpResult :: ResultBytes -> Ptr CZooOpResult -> IO ZooOpResult
peekZooSetOpResult mba ptr = do
  ret <- (#peek zoo_op_result_t, err) ptr
  ba <- Z.unsafeFreezeByteArray mba
  stat <- peekStat' $ castPtr $ Z.byteArrayContents ba
  return $ ZooSetOpResult ret stat

peekZooCheckOpResult :: Ptr CZooOpResult -> IO ZooOpResult
peekZooCheckOpResult ptr = ZooCheckOpResult <$> (#peek zoo_op_result_t, err) ptr
