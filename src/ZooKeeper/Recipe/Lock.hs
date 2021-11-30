module ZooKeeper.Recipe.Lock
  ( withLock

    -- * Internal
    -- Use them seperately is not safe!
  , lock
  , unlock
  ) where

import           Control.Exception      (bracket, catch, try)
import           Control.Monad
import           Z.Data.CBytes          (CBytes)

import           ZooKeeper
import           ZooKeeper.Exception    (ZNODEEXISTS, ZooException)
import           ZooKeeper.Recipe.Utils (SequenceNumWithGUID (..),
                                         createSeqEphemeralZNode,
                                         mkSequenceNumWithGUID)
import           ZooKeeper.Types

-- | To acquire a distributed lock.
-- Warning: do not forget to unlock it! Use 'withLock' instead if possible.
lock :: ZHandle
     -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
     -> CBytes
     -- ^ The path to get the lock. Ephemeral znodes will be put on it
     -> CBytes
     -- ^ The GUID for this zookeeper session. To handle recoverable execptions
     -- correctly, it should be distinct from different sessions
     -> IO CBytes
     -- ^ The real path of the lock that acquired. It will be used when unlocking
     -- the same lock
lock zk lockPath guid = do
  -- Check persistent paths
  do electionExists <- zooExists zk lockPath
     case electionExists of
       Just _  -> return ()
       Nothing -> void (try $ zooCreate zk lockPath Nothing zooOpenAclUnsafe ZooPersistent :: IO (Either ZooException StringCompletion))
     `catch` (\(_ :: ZNODEEXISTS) -> return ())

  -- Create Ephemeral and Sequece znode, and get the seq number i
  (StringCompletion this) <- createSeqEphemeralZNode zk lockPath guid
  let thisSeqNumWithGUID = mkSequenceNumWithGUID this

  callback zk thisSeqNumWithGUID
  where
    callback zk_ self = do
      (StringsCompletion (StringVector children)) <- zooGetChildren zk_ lockPath
      let childrenSeqNumWithGUID = mkSequenceNumWithGUID <$> children
      case filter (< self) childrenSeqNumWithGUID of
        [] -> do
          return $ unSequenceNumWithGUID self
        xs -> do
          let toWatch = lockPath <> "/" <> unSequenceNumWithGUID (maximum xs)
          zooWatchExists zk_ toWatch (\HsWatcherCtx{..} -> void $ callback watcherCtxZHandle self)
            (\_ -> return ())
          return $ unSequenceNumWithGUID self

-- | To release a distributed lock. Note that the real lock path
-- should be the one acquired by 'lock', otherwise, a 'ZooException'
-- will be thrown.
unlock :: ZHandle
       -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
       -> CBytes
       -- ^ The real lock path acquired by 'lock'. An exception will be
       -- thrown if it is bad (for example, does not exist)
       -> IO ()
unlock zk thisLock = zooDelete zk thisLock Nothing

-- | To do an action with a distributed lock. Only one caller with the same
-- 'lockPath' can execute the action at the same time. If the action throws
-- any exception during the locking period, the lock will be released and the
-- exception will be thrown again.
withLock :: ZHandle
         -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
         -> CBytes
         -- ^ The path to get the lock. Ephemeral znodes will be put on it.
         -> CBytes
         -- ^ The GUID for this zookeeper session. To handle recoverable execptions
         -- correctly, it should be distinct from different sessions.
         -> IO a
         -- ^ The action to be executed within the lock.
         -> IO a
withLock zk lockPath guid action =
  bracket (lock zk lockPath guid) (unlock zk) (const action)
