{-# LANGUAGE CPP #-}

module Database.ZooKeeper.Exception
  ( ZooException
  , zooExceptionToException
  , zooExceptionFromException
  , ZooExInfo (..)

    -- * System and server-side errors
    --
    -- $serverSideErrors
  , ZSYSTEMERROR          (..)
  , ZRUNTIMEINCONSISTENCY (..)
  , ZDATAINCONSISTENCY    (..)
  , ZCONNECTIONLOSS       (..)
  , ZMARSHALLINGERROR     (..)
  , ZUNIMPLEMENTED        (..)
  , ZOPERATIONTIMEOUT     (..)
  , ZBADARGUMENTS         (..)
  , ZINVALIDSTATE         (..)
  , ZNEWCONFIGNOQUORUM    (..)
  , ZRECONFIGINPROGRESS   (..)
  , ZSSLCONNECTIONERROR   (..)

    -- * API Errors
    --
    -- $apiErrors
  , ZAPIERROR                     (..)
  , ZNONODE                       (..)
  , ZNOAUTH                       (..)
  , ZBADVERSION                   (..)
  , ZNOCHILDRENFOREPHEMERALS      (..)
  , ZNODEEXISTS                   (..)
  , ZNOTEMPTY                     (..)
  , ZSESSIONEXPIRED               (..)
  , ZINVALIDCALLBACK              (..)
  , ZINVALIDACL                   (..)
  , ZAUTHFAILED                   (..)
  , ZCLOSING                      (..)
  , ZNOTHING                      (..)
  , ZSESSIONMOVED                 (..)
  , ZNOTREADONLY                  (..)
  , ZEPHEMERALONLOCALSESSION      (..)
  , ZNOWATCHER                    (..)
  , ZRECONFIGDISABLED             (..)
  , ZSESSIONCLOSEDREQUIRESASLAUTH (..)
  , ZTHROTTLEDOP                  (..)

    -- * Other Errors
  , SYSERRNO
  , UNKNOWN_ERR

  , throwZooError
  , throwZooErrorIfNotOK

    -- * Helpers
  , E.throwIO
  , getCErrNum
  ) where

import           Control.Exception (Exception (..))
import qualified Control.Exception as E
import           Data.Typeable     (cast)
import           Foreign.C         (CInt, CString)
import           GHC.Stack         (CallStack, HasCallStack, callStack,
                                    prettyCallStack)
import qualified Z.Data.Text       as T
import qualified Z.Data.Text.Print as T
import qualified Z.Foreign         as Z

-------------------------------------------------------------------------------

-- | The root exception type of ZooKeeper.
data ZooException = forall e . E.Exception e => ZooException e

instance Show ZooException where
  show (ZooException e) = show e

instance E.Exception ZooException

zooExceptionToException :: E.Exception e => e -> E.SomeException
zooExceptionToException = E.toException . ZooException

zooExceptionFromException :: E.Exception e => E.SomeException -> Maybe e
zooExceptionFromException x = do
  ZooException a <- E.fromException x
  cast a

-- | Zookeeper error informations.
data ZooExInfo = ZooExInfo
  { errDescription :: T.Text      -- ^ description for this zoo error
  , errCallStack   :: CallStack   -- ^ lightweight partial call-stack
  }

instance T.Print ZooExInfo where
  toUTF8BuilderP _ (ZooExInfo desc cstack) = do
    "description: "
    T.text desc
    ", callstack: "
    T.stringUTF8 (prettyCallStack cstack)

instance Show ZooExInfo where
  show = T.toString

#define MAKE_EX(e)                                \
newtype e = e ZooExInfo deriving (Show);          \
instance Exception e where                        \
{ toException = zooExceptionToException;          \
  fromException = zooExceptionFromException       \
}

-- $serverSideErrors
--
-- This is never thrown by the server, it shouldn't be used other than
-- to indicate a range. Specifically error codes greater than this
-- value, but lesser than 'ZAPIERROR', are system errors.
MAKE_EX(ZSYSTEMERROR         )
MAKE_EX(ZRUNTIMEINCONSISTENCY)
MAKE_EX(ZDATAINCONSISTENCY   )
MAKE_EX(ZCONNECTIONLOSS      )
MAKE_EX(ZMARSHALLINGERROR    )
MAKE_EX(ZUNIMPLEMENTED       )
MAKE_EX(ZOPERATIONTIMEOUT    )
MAKE_EX(ZBADARGUMENTS        )
MAKE_EX(ZINVALIDSTATE        )
MAKE_EX(ZNEWCONFIGNOQUORUM   )
MAKE_EX(ZRECONFIGINPROGRESS  )
MAKE_EX(ZSSLCONNECTIONERROR  )

-- $apiErrors
--
-- This is never thrown by the server, it shouldn't be used other than
-- to indicate a range. Specifically error codes greater than this
-- value are API errors (while values less than this indicate a
-- 'ZSYSTEMERROR').
MAKE_EX(ZAPIERROR                    )
MAKE_EX(ZNONODE                      )
MAKE_EX(ZNOAUTH                      )
MAKE_EX(ZBADVERSION                  )
MAKE_EX(ZNOCHILDRENFOREPHEMERALS     )
MAKE_EX(ZNODEEXISTS                  )
MAKE_EX(ZNOTEMPTY                    )
MAKE_EX(ZSESSIONEXPIRED              )
MAKE_EX(ZINVALIDCALLBACK             )
MAKE_EX(ZINVALIDACL                  )
MAKE_EX(ZAUTHFAILED                  )
MAKE_EX(ZCLOSING                     )
MAKE_EX(ZNOTHING                     )
MAKE_EX(ZSESSIONMOVED                )
MAKE_EX(ZNOTREADONLY                 )
MAKE_EX(ZEPHEMERALONLOCALSESSION     )
MAKE_EX(ZNOWATCHER                   )
MAKE_EX(ZRECONFIGDISABLED            )
MAKE_EX(ZSESSIONCLOSEDREQUIRESASLAUTH)
MAKE_EX(ZTHROTTLEDOP                 )

MAKE_EX(SYSERRNO)
MAKE_EX(UNKNOWN_ERR)

#define MAKE_THROW_EX(c, e)                                     \
throwZooError c stack = do                                      \
  desc <- T.validate <$> (Z.fromNullTerminated =<< c_zerror c); \
  E.throwIO $ e (ZooExInfo desc stack)

throwZooError :: CInt -> CallStack -> IO a
MAKE_THROW_EX((- 1), ZSYSTEMERROR         )
MAKE_THROW_EX((- 2), ZRUNTIMEINCONSISTENCY)
MAKE_THROW_EX((- 3), ZDATAINCONSISTENCY   )
MAKE_THROW_EX((- 4), ZCONNECTIONLOSS      )
MAKE_THROW_EX((- 5), ZMARSHALLINGERROR    )
MAKE_THROW_EX((- 6), ZUNIMPLEMENTED       )
MAKE_THROW_EX((- 7), ZOPERATIONTIMEOUT    )
MAKE_THROW_EX((- 8), ZBADARGUMENTS        )
MAKE_THROW_EX((- 9), ZINVALIDSTATE        )
MAKE_THROW_EX((-13), ZNEWCONFIGNOQUORUM   )
MAKE_THROW_EX((-14), ZRECONFIGINPROGRESS  )
MAKE_THROW_EX((-15), ZSSLCONNECTIONERROR  )
MAKE_THROW_EX((-100), ZAPIERROR                    )
MAKE_THROW_EX((-101), ZNONODE                      )
MAKE_THROW_EX((-102), ZNOAUTH                      )
MAKE_THROW_EX((-103), ZBADVERSION                  )
MAKE_THROW_EX((-108), ZNOCHILDRENFOREPHEMERALS     )
MAKE_THROW_EX((-110), ZNODEEXISTS                  )
MAKE_THROW_EX((-111), ZNOTEMPTY                    )
MAKE_THROW_EX((-112), ZSESSIONEXPIRED              )
MAKE_THROW_EX((-113), ZINVALIDCALLBACK             )
MAKE_THROW_EX((-114), ZINVALIDACL                  )
MAKE_THROW_EX((-115), ZAUTHFAILED                  )
MAKE_THROW_EX((-116), ZCLOSING                     )
MAKE_THROW_EX((-117), ZNOTHING                     )
MAKE_THROW_EX((-118), ZSESSIONMOVED                )
MAKE_THROW_EX((-119), ZNOTREADONLY                 )
MAKE_THROW_EX((-120), ZEPHEMERALONLOCALSESSION     )
MAKE_THROW_EX((-121), ZNOWATCHER                   )
MAKE_THROW_EX((-123), ZRECONFIGDISABLED            )
MAKE_THROW_EX((-124), ZSESSIONCLOSEDREQUIRESASLAUTH)
MAKE_THROW_EX((-127), ZTHROTTLEDOP                 )
throwZooError code stack
  | code > 0 = do
      desc <- T.validate <$> (Z.fromNullTerminated =<< c_zerror code)
      E.throwIO $ SYSERRNO (ZooExInfo desc stack)
  | otherwise =
      let codeBS = "UNKNOWN_ERR: " <> T.validate (T.toUTF8Bytes code)
       in E.throwIO $ UNKNOWN_ERR (ZooExInfo codeBS stack)

throwZooErrorIfNotOK :: HasCallStack => CInt -> IO CInt
throwZooErrorIfNotOK code
  | code == 0 = return 0
  | otherwise = throwZooError code callStack

-------------------------------------------------------------------------------

foreign import ccall unsafe "hs_zk.h zerror"
  c_zerror :: CInt -> IO CString

foreign import ccall unsafe "HsBase.h __hscore_get_errno"
  getCErrNum :: IO CInt
