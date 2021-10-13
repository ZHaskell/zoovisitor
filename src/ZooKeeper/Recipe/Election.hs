module ZooKeeper.Recipe.Election
  ( election
  ) where

import           Control.Monad
import qualified Z.Data.Builder         as B
import           Z.Data.CBytes          (CBytes)
import qualified Z.IO.Logger            as Log

import           ZooKeeper
import           ZooKeeper.Recipe.Utils (SequenceNumWithGUID (..),
                                         createSeqEphemeralZNode,
                                         mkSequenceNumWithGUID)
import           ZooKeeper.Types

-- | Run a leader election process.
-- __IMPORTANT__: This function may run endlessly until it is selected
-- as the leader.
election :: ZHandle
         -- ^ The zookeeper handle obtained by a call to 'zookeeperResInit'
         -> CBytes
         -- ^ The path to start the election from. Ephemeral znodes will be
         -- put on it
         -> CBytes
         -- ^ The GUID for this zookeeper session. To handle recoverable execptions
         -- correctly, it should be distinct from different sessions.
         -> IO ()
         -- ^ The action to be executed when an leader is elected.
         -> (DataCompletion -> IO ())
         -- ^ The action to be executed when a watcher is set. It can be used to
         -- remind the user that one 'step' is finished.
         -> IO ()
-- TODO: Use user-configurable logger instead
election zk electionPath guid leaderApp watchSetApp = Log.withDefaultLogger $ do
  let electionSeqPath = electionPath <> "/" <> guid <> "_"

  -- Check persistent paths
  electionExists <- zooExists zk electionPath
  case electionExists of
    Just _  -> return ()
    Nothing -> void $ zooCreate zk electionPath Nothing zooOpenAclUnsafe ZooPersistent

  -- Create Ephemeral and Sequece znode, and get the seq number i
  (StringCompletion this) <- createSeqEphemeralZNode zk electionPath guid
  let thisSeqNumWithGUID = mkSequenceNumWithGUID this
  Log.debug . B.stringUTF8 $ "Created SEQUENTIAL|EPHEMERAL ZNode " <> show thisSeqNumWithGUID

  -- Get the child that has the max seq number j < i
  (StringsCompletion (StringVector children)) <- zooGetChildren zk electionPath
  let childrenSeqNumWithGUID = mkSequenceNumWithGUID <$> children
  Log.debug . B.stringUTF8 $ "Children now: " <> show childrenSeqNumWithGUID

  -- find max j < i
  case filter (< thisSeqNumWithGUID) childrenSeqNumWithGUID of
    [] -> do
      let smallest = minimum childrenSeqNumWithGUID
      Log.debug . B.stringUTF8 $ "Leader elected: " <> show smallest
      leaderApp
    xs -> do
      let toWatch = electionPath <> "/" <> unSequenceNumWithGUID (maximum xs)
      Log.debug . B.stringUTF8 $ "Now watching: " <> show toWatch
      -- add watch
      zooWatchGet zk toWatch (callback electionSeqPath thisSeqNumWithGUID) watchSetApp
  where
    callback electionSeqPath thisSeqNumWithGUID HsWatcherCtx{..} = do
      Log.debug . B.stringUTF8 $ "Watch triggered, some node failed."
      (StringsCompletion (StringVector children)) <- zooGetChildren watcherCtxZHandle electionPath
      let childrenSeqNumWithGUID = mkSequenceNumWithGUID <$> children
      let smallest = minimum childrenSeqNumWithGUID
      case smallest == thisSeqNumWithGUID of
        True  -> do
          Log.debug . B.stringUTF8 $ "Leader elected: " <> show smallest
          leaderApp
        False -> do
          -- find max j < i
          case filter (< thisSeqNumWithGUID) childrenSeqNumWithGUID of
            [] -> Log.fatal . B.stringUTF8 $ "The 'impossible' happened!"
            xs -> do
              let toWatch = electionPath <> "/" <> unSequenceNumWithGUID (maximum xs )
              Log.debug . B.stringUTF8 $ "Now watching: " <> show toWatch
              -- add watch
              zooWatchGet zk toWatch (callback electionSeqPath thisSeqNumWithGUID) watchSetApp
