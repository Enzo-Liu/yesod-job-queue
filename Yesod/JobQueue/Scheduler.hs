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


-- | Cron Scheduler for YesodJobQueue
class (YesodJobQueue master) => YesodJobQueueScheduler master where
    -- | job schedules
    getJobSchedules :: master -> [(T.Text, JobType master)]

    -- | start schedule
    startJobSchedule :: (MonadBaseControl IO m, MonadIO m) => master -> m ()
    startJobSchedule master = do
        let add (s, jt) = flip addJob s $ do
              conn <- R.connect $ queueConnectInfo master
              enqueue master jt
        tids <- liftIO $ execSchedule $ mapM_ add $ getJobSchedules master
        liftIO $ print tids

-- | Need by 'getClassInformation'
schedulerInfo :: YesodJobQueueScheduler master => master ->  JobQueueClassInfo
schedulerInfo m = JobQueueClassInfo "Scheduler" $  map showSchedule $ getJobSchedules m
  where showSchedule (s, jt) = s <> " | " <> (T.pack . show $ jt)
