{-# LANGUAGE CPP #-}

module ZooKeeper.Exception
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
  , throwZooErrorIfLeft
  , throwZooErrorIfLeft'

    -- * Error number patterns
  , pattern CZOK
  , pattern CZSYSTEMERROR
  , pattern CZRUNTIMEINCONSISTENCY
  , pattern CZDATAINCONSISTENCY
  , pattern CZCONNECTIONLOSS
  , pattern CZMARSHALLINGERROR
  , pattern CZUNIMPLEMENTED
  , pattern CZOPERATIONTIMEOUT
  , pattern CZBADARGUMENTS
  , pattern CZINVALIDSTATE
  , pattern CZNEWCONFIGNOQUORUM
  , pattern CZRECONFIGINPROGRESS
  , pattern CZSSLCONNECTIONERROR
  , pattern CZAPIERROR
  , pattern CZNONODE
  , pattern CZNOAUTH
  , pattern CZBADVERSION
  , pattern CZNOCHILDRENFOREPHEMERALS
  , pattern CZNODEEXISTS
  , pattern CZNOTEMPTY
  , pattern CZSESSIONEXPIRED
  , pattern CZINVALIDCALLBACK
  , pattern CZINVALIDACL
  , pattern CZAUTHFAILED
  , pattern CZCLOSING
  , pattern CZNOTHING
  , pattern CZSESSIONMOVED
  , pattern CZNOTREADONLY
  , pattern CZEPHEMERALONLOCALSESSION
  , pattern CZNOWATCHER
  , pattern CZRECONFIGDISABLED
  , pattern CZSESSIONCLOSEDREQUIRESASLAUTH
  , pattern CZTHROTTLEDOP

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

pattern
    CZOK
  , CZSYSTEMERROR
  , CZRUNTIMEINCONSISTENCY
  , CZDATAINCONSISTENCY
  , CZCONNECTIONLOSS
  , CZMARSHALLINGERROR
  , CZUNIMPLEMENTED
  , CZOPERATIONTIMEOUT
  , CZBADARGUMENTS
  , CZINVALIDSTATE
  , CZNEWCONFIGNOQUORUM
  , CZRECONFIGINPROGRESS
  , CZSSLCONNECTIONERROR
  , CZAPIERROR
  , CZNONODE
  , CZNOAUTH
  , CZBADVERSION
  , CZNOCHILDRENFOREPHEMERALS
  , CZNODEEXISTS
  , CZNOTEMPTY
  , CZSESSIONEXPIRED
  , CZINVALIDCALLBACK
  , CZINVALIDACL
  , CZAUTHFAILED
  , CZCLOSING
  , CZNOTHING
  , CZSESSIONMOVED
  , CZNOTREADONLY
  , CZEPHEMERALONLOCALSESSION
  , CZNOWATCHER
  , CZRECONFIGDISABLED
  , CZSESSIONCLOSEDREQUIRESASLAUTH
  , CZTHROTTLEDOP
  :: CInt
pattern CZOK = 0
pattern CZSYSTEMERROR          = (- 1)
pattern CZRUNTIMEINCONSISTENCY = (- 2)
pattern CZDATAINCONSISTENCY    = (- 3)
pattern CZCONNECTIONLOSS       = (- 4)
pattern CZMARSHALLINGERROR     = (- 5)
pattern CZUNIMPLEMENTED        = (- 6)
pattern CZOPERATIONTIMEOUT     = (- 7)
pattern CZBADARGUMENTS         = (- 8)
pattern CZINVALIDSTATE         = (- 9)
pattern CZNEWCONFIGNOQUORUM    = (-13)
pattern CZRECONFIGINPROGRESS   = (-14)
pattern CZSSLCONNECTIONERROR   = (-15)
pattern CZAPIERROR                     = (-100)
pattern CZNONODE                       = (-101)
pattern CZNOAUTH                       = (-102)
pattern CZBADVERSION                   = (-103)
pattern CZNOCHILDRENFOREPHEMERALS      = (-108)
pattern CZNODEEXISTS                   = (-110)
pattern CZNOTEMPTY                     = (-111)
pattern CZSESSIONEXPIRED               = (-112)
pattern CZINVALIDCALLBACK              = (-113)
pattern CZINVALIDACL                   = (-114)
pattern CZAUTHFAILED                   = (-115)
pattern CZCLOSING                      = (-116)
pattern CZNOTHING                      = (-117)
pattern CZSESSIONMOVED                 = (-118)
pattern CZNOTREADONLY                  = (-119)
pattern CZEPHEMERALONLOCALSESSION      = (-120)
pattern CZNOWATCHER                    = (-121)
pattern CZRECONFIGDISABLED             = (-123)
pattern CZSESSIONCLOSEDREQUIRESASLAUTH = (-124)
pattern CZTHROTTLEDOP                  = (-127)

#define MAKE_EX(e)                                \
newtype e = e ZooExInfo deriving (Show);          \
instance Exception e where                        \
{ toException = zooExceptionToException;          \
  fromException = zooExceptionFromException }     \

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
MAKE_THROW_EX(CZSYSTEMERROR         , ZSYSTEMERROR         )
MAKE_THROW_EX(CZRUNTIMEINCONSISTENCY, ZRUNTIMEINCONSISTENCY)
MAKE_THROW_EX(CZDATAINCONSISTENCY   , ZDATAINCONSISTENCY   )
MAKE_THROW_EX(CZCONNECTIONLOSS      , ZCONNECTIONLOSS      )
MAKE_THROW_EX(CZMARSHALLINGERROR    , ZMARSHALLINGERROR    )
MAKE_THROW_EX(CZUNIMPLEMENTED       , ZUNIMPLEMENTED       )
MAKE_THROW_EX(CZOPERATIONTIMEOUT    , ZOPERATIONTIMEOUT    )
MAKE_THROW_EX(CZBADARGUMENTS        , ZBADARGUMENTS        )
MAKE_THROW_EX(CZINVALIDSTATE        , ZINVALIDSTATE        )
MAKE_THROW_EX(CZNEWCONFIGNOQUORUM   , ZNEWCONFIGNOQUORUM   )
MAKE_THROW_EX(CZRECONFIGINPROGRESS  , ZRECONFIGINPROGRESS  )
MAKE_THROW_EX(CZSSLCONNECTIONERROR  , ZSSLCONNECTIONERROR  )
MAKE_THROW_EX(CZAPIERROR                    , ZAPIERROR                    )
MAKE_THROW_EX(CZNONODE                      , ZNONODE                      )
MAKE_THROW_EX(CZNOAUTH                      , ZNOAUTH                      )
MAKE_THROW_EX(CZBADVERSION                  , ZBADVERSION                  )
MAKE_THROW_EX(CZNOCHILDRENFOREPHEMERALS     , ZNOCHILDRENFOREPHEMERALS     )
MAKE_THROW_EX(CZNODEEXISTS                  , ZNODEEXISTS                  )
MAKE_THROW_EX(CZNOTEMPTY                    , ZNOTEMPTY                    )
MAKE_THROW_EX(CZSESSIONEXPIRED              , ZSESSIONEXPIRED              )
MAKE_THROW_EX(CZINVALIDCALLBACK             , ZINVALIDCALLBACK             )
MAKE_THROW_EX(CZINVALIDACL                  , ZINVALIDACL                  )
MAKE_THROW_EX(CZAUTHFAILED                  , ZAUTHFAILED                  )
MAKE_THROW_EX(CZCLOSING                     , ZCLOSING                     )
MAKE_THROW_EX(CZNOTHING                     , ZNOTHING                     )
MAKE_THROW_EX(CZSESSIONMOVED                , ZSESSIONMOVED                )
MAKE_THROW_EX(CZNOTREADONLY                 , ZNOTREADONLY                 )
MAKE_THROW_EX(CZEPHEMERALONLOCALSESSION     , ZEPHEMERALONLOCALSESSION     )
MAKE_THROW_EX(CZNOWATCHER                   , ZNOWATCHER                   )
MAKE_THROW_EX(CZRECONFIGDISABLED            , ZRECONFIGDISABLED            )
MAKE_THROW_EX(CZSESSIONCLOSEDREQUIRESASLAUTH, ZSESSIONCLOSEDREQUIRESASLAUTH)
MAKE_THROW_EX(CZTHROTTLEDOP                 , ZTHROTTLEDOP                 )
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

throwZooErrorIfLeft :: HasCallStack => Either CInt a -> IO a
throwZooErrorIfLeft (Left rc) = throwZooError rc callStack
throwZooErrorIfLeft (Right x) = return x

throwZooErrorIfLeft' :: HasCallStack => (CInt -> Bool) -> Either CInt a -> IO (Maybe a)
throwZooErrorIfLeft' cond (Left rc) = if cond rc then return Nothing else throwZooError rc callStack
throwZooErrorIfLeft' _ (Right x)    = return $ Just x

-------------------------------------------------------------------------------

foreign import ccall unsafe "hs_zk.h zerror"
  c_zerror :: CInt -> IO CString

foreign import ccall unsafe "HsBase.h __hscore_get_errno"
  getCErrNum :: IO CInt
