{-# LANGUAGE OverloadedStrings #-}

import           Control.Monad   (void)
import           Test.Hspec
import           ZooKeeper
import           ZooKeeper.Types

main :: IO ()
main = hspec $ do
  describe "zookeeper get set" $ do
    it "set some value to a test node and get it" $ do
      let res = zookeeperResInit "127.0.0.1:2182" 5000 Nothing 0
      withResource res $ \zh -> do
        let nodeName = "/test-node"
        void $ zooCreate zh nodeName "" zooOpenAclUnsafe ZooPersistent
        void $ zooSet zh nodeName "hello" Nothing
        (dataCompletionValue <$> zooGet zh nodeName) `shouldReturn` "hello"
        zooDelete zh nodeName Nothing `shouldReturn` ()
