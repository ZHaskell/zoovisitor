module ZooKeeper.Recipe.Election
  ( election
  ) where

import           Control.Exception      (catch, throwIO)
import           Control.Monad
import           Z.Data.CBytes          (CBytes)

import           ZooKeeper
import           ZooKeeper.Exception    (ZNODEEXISTS)
import           ZooKeeper.Recipe.Utils (ZkRecipeException (..),
                                         SequenceNumWithGUID (..),
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
election zk electionPath guid leaderApp watchSetApp = do
  let electionSeqPath = electionPath <> "/" <> guid <> "_"

  -- Check persistent paths
  do electionExists <- zooExists zk electionPath
     case electionExists of
       Just _  -> return ()
       Nothing -> void $ zooCreate zk electionPath Nothing zooOpenAclUnsafe ZooPersistent
     `catch` (\(_ :: ZNODEEXISTS) -> return ())

  -- Create Ephemeral and Sequece znode, and get the seq number i
  (StringCompletion this) <- createSeqEphemeralZNode zk electionPath guid
  let thisSeqNumWithGUID = mkSequenceNumWithGUID this
  -- TODO: Use zookeeper log
  -- Log.debug . B.stringUTF8 $ "Created SEQUENTIAL|EPHEMERAL ZNode " <> show thisSeqNumWithGUID

  -- Get the child that has the max seq number j < i
  (StringsCompletion (StringVector children)) <- zooGetChildren zk electionPath
  let childrenSeqNumWithGUID = mkSequenceNumWithGUID <$> children
  -- TODO: Use zookeeper log
  -- Log.debug . B.stringUTF8 $ "Children now: " <> show childrenSeqNumWithGUID

  -- find max j < i
  case filter (< thisSeqNumWithGUID) childrenSeqNumWithGUID of
    [] -> do
      let _smallest = minimum childrenSeqNumWithGUID
      -- TODO: Use zookeeper log
      -- Log.debug . B.stringUTF8 $ "Leader elected: " <> show smallest
      leaderApp
    xs -> do
      let toWatch = electionPath <> "/" <> unSequenceNumWithGUID (maximum xs)
      -- TODO: Use zookeeper log
      -- Log.debug . B.stringUTF8 $ "Now watching: " <> show toWatch
      -- add watch
      zooWatchGet zk toWatch (callback electionSeqPath thisSeqNumWithGUID) watchSetApp
  where
    callback electionSeqPath thisSeqNumWithGUID HsWatcherCtx{..} = do
      -- TODO: Use zookeeper log
      --Log.debug . B.stringUTF8 $ "Watch triggered, some node failed."
      (StringsCompletion (StringVector children)) <- zooGetChildren watcherCtxZHandle electionPath
      let childrenSeqNumWithGUID = mkSequenceNumWithGUID <$> children
      let smallest = minimum childrenSeqNumWithGUID
      if smallest == thisSeqNumWithGUID
         then do
           -- TODO: Use zookeeper log
           --Log.debug . B.stringUTF8 $ "Leader elected: " <> show smallest
           leaderApp
         else do
           -- find max j < i
           case filter (< thisSeqNumWithGUID) childrenSeqNumWithGUID of
             [] -> throwIO $ ZkRecipeException "The 'impossible' happened!"
             xs -> do
               let toWatch = electionPath <> "/" <> unSequenceNumWithGUID (maximum xs )
               -- Log.debug . B.stringUTF8 $ "Now watching: " <> show toWatch
               -- add watch
               zooWatchGet zk toWatch (callback electionSeqPath thisSeqNumWithGUID) watchSetApp
