{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Concurrent
import           Control.Monad       (void)
import           Data.Version        (makeVersion)
import           Test.Hspec
import           ZooKeeper
import           ZooKeeper.Exception
import           ZooKeeper.Types

client :: Resource ZHandle
client = zookeeperResInit "127.0.0.1:2182" 5000 Nothing 0

main :: IO ()
main = withResource client $ \zh -> do
  hspec $ smoke zh
  hspec $ multiSpec zh

smoke :: ZHandle -> Spec
smoke zh = do
  describe "ZooKeeper.zooVersion" $ do
    it "version should be 3.4.*" $ do
      zooVersion `shouldSatisfy` (>= makeVersion [3, 4, 0])
      zooVersion `shouldSatisfy` (< makeVersion [3, 5, 0])

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
    it "wathch for node to appear" $ do
      ctx <- newEmptyMVar
      ret <- newEmptyMVar
      _ <- forkIO $ zooWatchExists zh "/b" (ctx `putMVar`) (ret `putMVar`)
      takeMVar ret `shouldReturn` Nothing
      _ <- forkIO $ void $ zooCreate zh "/b" Nothing zooOpenAclUnsafe ZooEphemeral
      ctx' <- takeMVar ctx
      watcherCtxType ctx' `shouldBe` ZooCreateEvent
      watcherCtxState ctx' `shouldBe` ZooConnectedState
      watcherCtxPath ctx' `shouldBe` Just "/b"

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

multiSpec :: ZHandle -> Spec
multiSpec zh = describe "ZooKeeper.zooMulti" $ do
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

-------------------------------------------------------------------------------

noChildrenForEphemerals :: Selector ZNOCHILDRENFOREPHEMERALS
noChildrenForEphemerals = const True
