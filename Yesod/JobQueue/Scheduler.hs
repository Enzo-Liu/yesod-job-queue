-- | Cron Job for Yesod
module Yesod.JobQueue.Scheduler where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Control (MonadBaseControl)
import Data.Monoid ((<>))
import qualified Data.Text as T (pack, Text)
import System.Cron.Schedule
import Yesod.JobQueue
import Yesod.JobQueue.Types
import qualified Database.Redis as R
import qualified Database.Redis.Utils as R
import qualified Data.ByteString.Char8 as B

import Control.Monad(when)


-- | Cron Scheduler for YesodJobQueue
class (YesodJobQueue master) => YesodJobQueueScheduler master where
    -- | job schedules
    getJobSchedules :: master -> [(T.Text, JobType master)]

    -- | start schedule
    startJobSchedule :: (MonadBaseControl IO m, MonadIO m) => master -> m ()
    startJobSchedule master = do
        let add (s, jt) = flip addJob s $ do
              let key = queueKey master
                  jts = B.pack $ show jt
              conn <- R.connect $ queueConnectInfo master
              locked <- R.runRedis conn $ R.acquireLock key 10 jts
              when locked $ do
                enqueue master jt
                R.runRedis conn $ R.releaseLock key jts
        tids <- liftIO $ execSchedule $ mapM_ add $ getJobSchedules master
        liftIO $ print tids

-- | Need by 'getClassInformation'
schedulerInfo :: YesodJobQueueScheduler master => master ->  JobQueueClassInfo
schedulerInfo m = JobQueueClassInfo "Scheduler" $  map showSchedule $ getJobSchedules m
  where showSchedule (s, jt) = s <> " | " <> (T.pack . show $ jt)
