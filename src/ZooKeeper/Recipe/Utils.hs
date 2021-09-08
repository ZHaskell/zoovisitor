module ZooKeeper.Recipe.Utils
  ( -- * Types
    SequenceNumWithGUID(..)
  , mkSequenceNumWithGUID
  , extractSeqNum

    -- * ZNode operations
  , createSeqEphemeralZNode
  ) where

import           Control.Exception
import qualified Data.List           as L
import           Z.Data.CBytes       (CBytes)
import qualified Z.Data.CBytes       as CB
import           ZooKeeper
import           ZooKeeper.Exception
import           ZooKeeper.Types

--------------------------------------------------------------------------------

-- | Represenets a name of a SEQUENCE|EPHEMERAL znode. It contains two parts,
-- a GUID, and a sequence number. The GUID is used for handleing recoverable
-- exceptions so we only care about the sequence number part when we comparing
-- two of them.
newtype SequenceNumWithGUID = SequenceNumWithGUID
  { unSequenceNumWithGUID :: CBytes
  }

mkSequenceNumWithGUID :: CBytes -> SequenceNumWithGUID
mkSequenceNumWithGUID = SequenceNumWithGUID

instance Eq SequenceNumWithGUID where
  (SequenceNumWithGUID s1) == (SequenceNumWithGUID s2) =
    extractSeqNum s1 == extractSeqNum s2

instance Ord SequenceNumWithGUID where
  (SequenceNumWithGUID s1) <= (SequenceNumWithGUID s2) =
    extractSeqNum s1 <= extractSeqNum s2

instance Show SequenceNumWithGUID where
  show (SequenceNumWithGUID s) = CB.unpack s

-- | Exrtact only the sequence number part from an `SequenceNumWithGUID`.
extractSeqNum :: CBytes -> CBytes
extractSeqNum = CB.pack . reverse . takeWhile (/= '_') . reverse . CB.unpack

--------------------------------------------------------------------------------

-- | Creates a sequential and ephemeral znode with specified prefix
-- and GUID. The created znode is as `prefixPath/GUID-n_0000000001`.
-- Note that it uses a GUID to handle recoverable exceptions, see
-- [this](https://zookeeper.apache.org/doc/r3.7.0/recipes.html#sc_recipes_GuidNote)
-- for more details.
createSeqEphemeralZNode :: ZHandle -> CBytes -> CBytes -> IO StringCompletion
createSeqEphemeralZNode zk prefixPath guid = do
  let seqPath = prefixPath <> "/" <> guid <> "_"
  catches (zooCreate zk seqPath Nothing zooOpenAclUnsafe ZooEphemeralSequential)
    [ Handler (\(_ :: ZCONNECTIONLOSS  ) -> retry)
    , Handler (\(_ :: ZOPERATIONTIMEOUT) -> retry)
    ]
  where
    retry :: IO StringCompletion
    retry = do
      (StringsCompletion (StringVector children)) <- zooGetChildren zk prefixPath
      case L.find (\child -> CB.unpack guid `L.isSubsequenceOf` CB.unpack child) children of
        Just child -> return $ StringCompletion child
        Nothing    -> createSeqEphemeralZNode zk prefixPath guid
