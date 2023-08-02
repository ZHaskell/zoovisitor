{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import           Control.Concurrent
import           Control.Concurrent.Async (concurrently_)
import           Control.Monad            (replicateM, void)
import qualified Data.UUID                as UUID
import           Data.UUID.V4
import           Data.Version             (makeVersion)
import           Foreign.C
import           Test.Hspec
import qualified Z.Data.CBytes            as CB

import           ZooKeeper
import           ZooKeeper.Exception
import           ZooKeeper.Recipe         (election, lock, unlock)
import           ZooKeeper.Types

recvTimeout :: CInt
recvTimeout = 5000

client :: Resource ZHandle
client = zookeeperResInit "127.0.0.1:2182" Nothing recvTimeout Nothing 0

main :: IO ()
main = withResource client $ \zh -> do
  hspec $ opSpec zh
  hspec $ multiOpSpec zh
  hspec $ propSpec zh
  hspec $ electionSpec1 zh
  hspec $ electionSpec2 client
  hspec $ lockSpec1 zh
  hspec $ lockSpec2 client

opSpec :: ZHandle -> Spec
opSpec zh = do
  describe "ZooKeeper.zooVersion" $ do
    it "version should be 3.4.* - 3.8.*" $ do
      putStrLn $ "Tested with zooVersion: " <> show zooVersion
      zooVersion `shouldSatisfy` (>= makeVersion [3, 4, 0])
      zooVersion `shouldSatisfy` (<= makeVersion [3, 8, 0])

  describe "zookeeper get set" $ do
    it "set some value to a node and get it" $ do
      let nodeName = "/test-node"
      void $ zooCreate zh nodeName Nothing zooOpenAclUnsafe ZooEphemeral
      void $ zooSet zh nodeName (Just "hello") Nothing
      (dataCompletionValue <$> zooGet zh nodeName) `shouldReturn` Just "hello"

  describe "ZooKeeper.zooGet" $ do
    it "get an empty node: the value should be Nothing" $ do
      _ <- zooCreate zh "/a" Nothing zooOpenAclUnsafe ZooEphemeral
      dataCompletionValue <$> zooGet zh "/a" `shouldReturn` Nothing

  describe "ZooKeeper.zooWatchExists" $ do
    it "watch for node to appear" $ do
      ctx <- newEmptyMVar
      ret <- newEmptyMVar
      _ <- forkIO $ zooWatchExists zh "/b" (ctx `putMVar`) (ret `putMVar`)
      takeMVar ret `shouldReturn` Nothing
      _ <- forkIO $ void $ zooCreate zh "/b" Nothing zooOpenAclUnsafe ZooEphemeral
      ctx' <- takeMVar ctx
      watcherCtxType ctx' `shouldBe` ZooCreateEvent
      watcherCtxState ctx' `shouldBe` ZooConnectedState
      watcherCtxPath ctx' `shouldBe` Just "/b"

  describe "ZooKeeper.zooDeleteAll" $ do
    it "test recursively delete" $ do
      void $ zooCreate zh "/x" Nothing zooOpenAclUnsafe ZooPersistent
      void $ zooCreate zh "/x/1" Nothing zooOpenAclUnsafe ZooPersistent
      void $ zooCreate zh "/x/2" Nothing zooOpenAclUnsafe ZooPersistent
      void $ zooCreate zh "/x/2/1" Nothing zooOpenAclUnsafe ZooPersistent
      unStrVec . strsCompletionValues <$> zooGetChildren zh "/x" `shouldReturn` ["1", "2"]
      zooDeleteAll zh "/x" `shouldReturn` ()
      void $ zooCreate zh "/x" Nothing zooOpenAclUnsafe ZooPersistent
      zooDeleteAll zh "/x" `shouldReturn` ()

  describe "ZooKeeper.zooGetChildren" $ do
    it "test get children" $ do
      void $ zooCreate zh "/x" Nothing zooOpenAclUnsafe ZooPersistent
      void $ zooCreate zh "/x/1" Nothing zooOpenAclUnsafe ZooPersistent
      void $ zooCreate zh "/x/2" Nothing zooOpenAclUnsafe ZooPersistent
      unStrVec . strsCompletionValues <$> zooGetChildren zh "/x" `shouldReturn` ["1", "2"]
      zooDelete zh "/x/1" Nothing `shouldReturn` ()
      zooDelete zh "/x/2" Nothing `shouldReturn` ()
      zooDelete zh "/x" Nothing `shouldReturn` ()

    -- FIXME: Allow ephemeral znodes to have children created only by the owner session
    -- https://issues.apache.org/jira/browse/ZOOKEEPER-834
    it "create children of ephemeral nodes should throw exception" $ do
      void $ zooCreate zh "/x" Nothing zooOpenAclUnsafe ZooEphemeral
      zooCreate zh "/x/1" Nothing zooOpenAclUnsafe ZooEphemeral `shouldThrow` noChildrenForEphemerals

    it "get children of a leaf node should return []" $ do
      void $ zooCreate zh "/y" Nothing zooOpenAclUnsafe ZooPersistent
      unStrVec . strsCompletionValues <$> zooGetChildren zh "/y" `shouldReturn` []
      zooDelete zh "/y" Nothing `shouldReturn` ()

  describe "ZooKeeper.zooGetAcl" $ do
    it "get acl of READ permission" $ do
      void $ zooCreate zh "/acl1" Nothing zooReadAclUnsafe ZooEphemeral
      compactZooPerms . aclPerms . head . aclCompletionAcls <$> zooGetAcl zh "/acl1"
        `shouldReturn` ZooPermRead

    it "get acl of ALL permission" $ do
      void $ zooCreate zh "/acl2" Nothing zooOpenAclUnsafe ZooEphemeral
      compactZooPerms . aclPerms . head . aclCompletionAcls <$> zooGetAcl zh "/acl2"
        `shouldReturn` ZooPermAll

propSpec :: ZHandle -> Spec
propSpec zh = do
  describe "ZooKeeper.zooState" $ do
    it "test get state" $ do
      zooState zh `shouldReturn` ZooConnectedState

  describe "ZooKeeper.zooClientID" $ do
    it "test get data of ClientID" $ do
      zooState zh `shouldReturn` ZooConnectedState
      HsClientID{..} <- peekClientId =<< zooClientID zh
      clientId `shouldNotBe` 0

  describe "ZooKeeper.zooRecvTimeout" $ do
    it "test receive timeout" $ do
      zooRecvTimeout zh `shouldReturn` recvTimeout

multiOpSpec :: ZHandle -> Spec
multiOpSpec zh = describe "ZooKeeper.zooMulti" $ do
  it "Test basic multi-op functionality" $ do
    let op0 = zooCreateOpInit "/multi" (Just "") 64 zooOpenAclUnsafe ZooPersistent
    let op1 = zooCreateOpInit "/multi/a" (Just "") 64 zooOpenAclUnsafe ZooPersistent
    let op2 = zooSetOpInit "/multi/a" (Just "hello") Nothing
    let op3 = zooDeleteOpInit "/multi/a" Nothing
    let op4 = zooDeleteOpInit "/multi" Nothing

    results <- zooMulti zh [op0, op1, op2]
    head results `shouldBe` ZooCreateOpResult CZOK "/multi"
    results !! 1 `shouldBe` ZooCreateOpResult CZOK "/multi/a"
    case results !! 2 of
      ZooSetOpResult ret _ -> ret `shouldBe` CZOK
      _                    -> error "Invalid Op Result"
    (dataCompletionValue <$> zooGet zh "/multi/a") `shouldReturn` Just "hello"
    results' <- zooMulti zh [op3, op4]
    head results' `shouldBe` ZooDeleteOpResult CZOK
    results' !! 1 `shouldBe` ZooDeleteOpResult CZOK
    zooExists zh "/multi" `shouldReturn` Nothing

electionSpec1 :: ZHandle -> Spec
electionSpec1 zh = describe "ZooKeeper.zooElection (single session)" $ do
  it "Test single node situation" $ do
    leader <- newEmptyMVar
    uuid <- nextRandom
    _ <- forkIO $ election zh "/election1" (CB.pack . UUID.toString $ uuid)
      (putMVar leader 1) (\_ -> return ())
    readMVar leader `shouldReturn` (1 :: Int)

electionSpec2 :: Resource ZHandle -> Spec
electionSpec2 client_ = describe "ZooKeeper.zooElection (multi sessions)" $ do
  it "Test multi node situation" $ do
    leader <- newMVar 0
    [finished1, finished2, finished3, finished4] <- replicateM 4 newEmptyMVar
    [uuid1, uuid2, uuid3] <- replicateM 3 nextRandom

    t1 <- forkIO $ withResource client_ $ \zh1 -> do
      t <- election zh1 "/election2" (CB.pack . UUID.toString $ uuid1)
        (do {void $ swapMVar leader 1; putMVar finished1 ()})
        (\_ -> putMVar finished1 ())
      threadDelay (maxBound :: Int)
      return t
    void $ takeMVar finished1

    _ <- forkIO $ withResource client_ $ \zh2 -> do
      election zh2 "/election2" (CB.pack . UUID.toString $ uuid2)
        (do {void $ swapMVar leader 2; putMVar finished4 ()})
        (\_ -> putMVar finished2 ())
    void $ takeMVar finished2

    _ <- forkIO $ withResource client_ $ \zh3 -> do
      election zh3 "/election2" (CB.pack . UUID.toString $ uuid3)
        (void $ swapMVar leader 3)
        (\_ -> putMVar finished3 ())
    void $ takeMVar finished3

    killThread t1
    takeMVar finished4
    readMVar leader `shouldReturn` (2 :: Int)

lockSpec1 :: ZHandle -> Spec
lockSpec1 zh = describe "ZooKeeper.zooLock (single session)" $ do
  it "Test single client situation" $ do
    result <- newEmptyMVar
    uuid <- nextRandom
    thisLock <- lock zh "/lock1" (CB.pack . UUID.toString $ uuid)
    putMVar result 1
    unlock zh thisLock
    readMVar result `shouldReturn` (1 :: Int)

lockSpec2 :: Resource ZHandle -> Spec
lockSpec2 client_ = describe "ZooKeeper.zooLock (multi sessions)" $ do
  it "Test multi clients situation" $ do
    result1 <- newMVar 0
    result2 <- newMVar 0
    [uuid1, uuid2] <- replicateM 2 nextRandom

    concurrently_ (action1 result1) (action1 result1)
    readMVar result1 `shouldReturn` (1 :: Int)

    withResource client_ $ \zh1 -> do
      withResource client_ $ \zh2 -> do
        concurrently_ (action2 zh1 (CB.pack . UUID.toString $ uuid1) result2)
                      (action2 zh2 (CB.pack . UUID.toString $ uuid2) result2)
        readMVar result2 `shouldReturn` (2 :: Int)
  where
    action1 target = do
      x <- readMVar target
      threadDelay 100000
      swapMVar target (x + 1)
    action2 zh guid target = do
      thisLock <- lock zh "/lock2" guid
      x <- readMVar target
      threadDelay 100000
      _ <- swapMVar target (x + 1)
      unlock zh thisLock

-------------------------------------------------------------------------------

noChildrenForEphemerals :: Selector ZNOCHILDRENFOREPHEMERALS
noChildrenForEphemerals = const True
