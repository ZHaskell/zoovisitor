module ZooKeeper.Recipe.Lock
  ( lock
  , unlock
  ) where

import           Control.Exception      (catch, try)
import           Control.Monad
import           Z.Data.CBytes          (CBytes)

import           ZooKeeper
import           ZooKeeper.Exception    (ZNODEEXISTS, ZooException)
import           ZooKeeper.Recipe.Utils (SequenceNumWithGUID (..),
                                         createSeqEphemeralZNode,
                                         mkSequenceNumWithGUID)
import           ZooKeeper.Types

-- | To acquire a distributed lock.
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
