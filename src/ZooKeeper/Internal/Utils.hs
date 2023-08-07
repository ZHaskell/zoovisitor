module ZooKeeper.Internal.Utils
  ( -- * Resource management
    Resource (..)
  , initResource
  , initResource_
  , withResource
  , withResource'
  ) where

import           Control.Exception      (onException)
import           Control.Monad          (when)
import qualified Control.Monad.Catch    as MonadCatch
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           GHC.Stack              (HasCallStack)
import           Z.Data.PrimRef         (atomicOrCounter, newCounter)

--------------------------------------------------------------------------------

-- | A 'Resource' is an 'IO' action which acquires some resource of type a and
-- also returns a finalizer of type IO () that releases the resource.
--
-- The only safe way to use a 'Resource' is 'withResource' and 'withResource'',
-- You should not use the 'acquire' field directly, unless you want to implement
-- your own resource management. In the later case, you should 'mask_' 'acquire'
-- since some resource initializations may assume async exceptions are masked.
--
-- 'MonadIO' instance is provided so that you can lift 'IO' computation inside
-- 'Resource', this is convenient for propagating 'Resource' around since many
-- 'IO' computations carry finalizers.
newtype Resource a = Resource { acquire :: IO (a, IO ()) }

-- | Create 'Resource' from create and release action.
--
-- Note, 'resource' doesn't open resource itself, resource is created when you
-- use 'with' \/ 'with''.
initResource :: IO a -> (a -> IO ()) -> Resource a
{-# INLINABLE initResource #-}
initResource create release = Resource $ do
    r <- create
    return (r, release r)

-- | Create 'Resource' from create and release action.
--
-- This function is useful when you want to add some initialization and clean
-- up action inside 'Resource' monad.
initResource_ :: IO a -> IO () -> Resource a
{-# INLINABLE initResource_ #-}
initResource_ create release = Resource $ do
    r <- create
    return (r, release)

instance Functor Resource where
    {-# INLINE fmap #-}
    fmap f resource = Resource $ do
        (a, release) <- acquire resource
        return (f a, release)

instance Applicative Resource where
    {-# INLINE pure #-}
    pure a = Resource (pure (a, pure ()))
    {-# INLINE (<*>) #-}
    resource1 <*> resource2 = Resource $ do
        (f, release1) <- acquire resource1
        (x, release2) <- acquire resource2 `onException` release1
        return (f x, release2 >> release1)

instance Monad Resource where
    {-# INLINE return #-}
    return = pure
    {-# INLINE (>>=) #-}
    m >>= f = Resource $ do
        (m', release1) <- acquire m
        (x , release2) <- acquire (f m') `onException` release1
        return (x, release2 >> release1)

instance MonadIO Resource where
    {-# INLINE liftIO #-}
    liftIO f = Resource $ fmap (\ a -> (a, dummyRelease)) f
        where dummyRelease = return ()

-- | Create a new resource and run some computation, resource is guarantee to
-- be closed.
--
-- Be careful, don't leak the resource through the computation return value
-- because after the computation finishes, the resource is already closed.
withResource :: (MonadCatch.MonadMask m, MonadIO m, HasCallStack)
             => Resource a -> (HasCallStack => a -> m b) -> m b
{-# INLINABLE withResource #-}
withResource resource k = MonadCatch.bracket
    (liftIO (acquire resource))
    (\(_, release) -> liftIO release)
    (\(a, _) -> k a)

-- | Create a new resource and run some computation, resource is guarantee to
-- be closed.
--
-- The difference from 'with' is that the computation will receive an extra
-- close action, which can be used to close the resource early before the whole
-- computation finished, the close action can be called multiple times,
-- only the first call will clean up the resource.
withResource' :: (MonadCatch.MonadMask m, MonadIO m, HasCallStack)
              => Resource a -> (HasCallStack => a -> m () -> m b) -> m b
{-# INLINABLE withResource' #-}
withResource' resource k = do
    c <- liftIO (newCounter 0)
    MonadCatch.bracket
        (liftIO $ do
            (a, release) <- acquire resource
            let release' = do
                    c' <- atomicOrCounter c 1
                    when (c' == 0) release
            return (a, release'))
        (\(_, release) -> liftIO release)
        (\(a, release) -> k a (liftIO release))
