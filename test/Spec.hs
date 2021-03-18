{-# LANGUAGE OverloadedStrings #-}

import           Control.Concurrent
import           Control.Monad      (void)
import           Data.Version       (makeVersion)
import           Test.Hspec
import           ZooKeeper
import           ZooKeeper.Types

client :: Resource ZHandle
client = zookeeperResInit "127.0.0.1:2182" 5000 Nothing 0

main :: IO ()
main = withResource client $ \zh -> hspec $ do
  describe "ZooKeeper.zooVersion" $ do
    it "version should be 3.4.*" $ do
      zooVersion `shouldSatisfy` (>= makeVersion [3, 4, 0])
      zooVersion `shouldSatisfy` (< makeVersion [3, 5, 0])

  describe "zookeeper get set" $ do
    it "set some value to a test node and get it" $ do
      let nodeName = "/test-node"
      void $ zooCreate zh nodeName Nothing zooOpenAclUnsafe ZooPersistent
      void $ zooSet zh nodeName "hello" Nothing
      (dataCompletionValue <$> zooGet zh nodeName) `shouldReturn` Just "hello"
      zooDelete zh nodeName Nothing `shouldReturn` ()

  describe "ZooKeeper.zooGet" $ do
    it "get an empty node" $ do
      _ <- zooCreate zh "/a" Nothing zooOpenAclUnsafe ZooPersistent
      dataCompletionValue <$> zooGet zh "/a" `shouldReturn` Nothing
      zooDelete zh "/a" Nothing `shouldReturn` ()

  describe "ZooKeeper.zooWatchExists" $ do
    it "wathch for node to appear" $ do
      ctx <- newEmptyMVar
      ret <- newEmptyMVar
      _ <- forkIO $ zooWatchExists zh "/b" (ctx `putMVar`) (ret `putMVar`)
      takeMVar ret `shouldReturn` Nothing
      _ <- forkIO $ void $ zooCreate zh "/b" Nothing zooOpenAclUnsafe ZooPersistent
      ctx' <- takeMVar ctx
      watcherCtxType ctx' `shouldBe` ZooCreateEvent
      watcherCtxState ctx' `shouldBe` ZooConnectedState
      watcherCtxPath ctx' `shouldBe` Just "/b"
      zooDelete zh "/b" Nothing `shouldReturn` ()
