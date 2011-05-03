import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class ActivityProfiler {
    private final static String LOCATION_PATH = "/home/rokey/Work/workspace/activityProfiler/locations/";

//    private final static String FILE_NAME = "locations-ch1.csv";
    private final static String FILE_NAME = "locations.csv";

    private static final long DURATION_THRESHOLD = 300000; // 5 minutes in
                                                           // millisecs

    private static final long MERGE_THRESHOLD = 3600000; // 1 hour in millisecs

    private static final int TRANSITION_COUNT_THRESHOLD = 5; // 3 transition to
                                                             // an already
                                                             // identified
                                                             // activity

    private static final long MAJOR_MERGE_THRESHOLD = 86400000; // 24 hrs

    private static final int TRANSITION_TOTAL_COUNT_THRESHOLD = 10; // 10
                                                                   // transitions
                                                                   // within 24
                                                                   // hrs

    private static HashSet<Integer> mAllCids = new HashSet<Integer>();
    
    public static void main(String[] args) {
        File locationFile = new File(LOCATION_PATH + FILE_NAME);
        if (!locationFile.exists()) {
            return;
        }

        FileReader in;
        try {
            in = new FileReader(locationFile);
            BufferedReader reader = new BufferedReader(in);
            String line;
            boolean firstline = true;
            while ((line = reader.readLine()) != null) {
                if (firstline) {
                    CidStats cStat = new CidStats();
                    cStat.setCid(-1);
                    mCidStats.put(-1, cStat);
                    firstline = false;
                    continue;
                }

                parseline(line);
            }

            in.close();
            
            System.out.println("Total cid number including -1 " + mAllCids.size());
            
//            printCidLocations();
            
//            printActivitiesLocations();
            
            printActivitiesForMatLab();

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void printActivitiesForMatLab() {
        Iterator<String> iterator = mActivities.keySet().iterator();
        while (iterator.hasNext()){
            StringBuffer strBuf = new StringBuffer();
            String actId = iterator.next();
            ActivityStats actStats = mActivities.get(actId);            
            Iterator<Integer> cidIter = actStats.mCids.iterator();
            while (cidIter.hasNext()){
                int cid = cidIter.next();
                CidStats cStat = mCidStats.get(cid);
                strBuf.append(matlabPrintLocList(actId, cStat.getLocations()));                
            }
//            System.out.println("<" + actId + ">");
            System.out.print(strBuf.toString());
        }
    }

    private static Object matlabPrintLocList(String actId, ArrayList<LatLong> locList) {
        StringBuffer strBuf = new StringBuffer();
        Iterator<LatLong> iterator = locList.iterator();
        while (iterator.hasNext()){
            LatLong latLong = iterator.next();
            strBuf.append(actId);
            strBuf.append("\t");
            strBuf.append(latLong.latitude);
            strBuf.append("\t");
            strBuf.append(latLong.longitude);
            strBuf.append("\n");
        }
        return strBuf.toString();
    }

    private static void printActivitiesLocations() {
        System.out.println("Activities locations:");
        Iterator<String> iterator = mActivities.keySet().iterator();
        while (iterator.hasNext()){
            StringBuffer strBuf = new StringBuffer();
            String actId = iterator.next();
            ActivityStats actStats = mActivities.get(actId);            
            Iterator<Integer> cidIter = actStats.mCids.iterator();
            while (cidIter.hasNext()){
                int cid = cidIter.next();
                CidStats cStat = mCidStats.get(cid);
                strBuf.append(prettyPrintLocList(cid, cStat.getLocations()));                
            }
            System.out.println("<" + actId + ">");
            System.out.println(strBuf.toString());
        }
    }
    
    private static String prettyPrintLocList(Integer cid, ArrayList<LatLong> locList){
        StringBuffer strBuf = new StringBuffer();
        Iterator<LatLong> iterator = locList.iterator();
        while (iterator.hasNext()){
            LatLong latLong = iterator.next();
            strBuf.append(cid);
            strBuf.append("\t");
            strBuf.append(latLong.latitude);
            strBuf.append("\t");
            strBuf.append(latLong.longitude);
            strBuf.append("\n");
        }
        return strBuf.toString();
    }

    private static void printCidLocations() {
        System.out.println("Cid locations:");
        Iterator<Integer> iterator = mCidStats.keySet().iterator();
        while (iterator.hasNext()){
            int cid = iterator.next();
            CidStats cStats = mCidStats.get(cid);
            System.out.println("<"+cid+">");
            System.out.println(prettyPrintLocList(cid, cStats.getLocations()));
        }
    }

    private static HashMap<Integer, CidStats> mCidStats = new HashMap<Integer, CidStats>();

    private static int mLastCid = -1;

    private static long mLastEpochTime = 1302079027648L;

    private static long mLastMergeTime;

    private static HashSet<Integer> mUnprocessedCids = new HashSet<Integer>();

    private static long mLastMajorMerge;

    private static HashMap<String, ActivityStats> mActivities = new HashMap<String, ActivityStats>();

    private static void parseline(String line) {
        String[] splits = line.split(";");
        if (splits.length != 10) {
            System.out.println("Incorrect line " + line);
        }

        try {
            int sequence = Integer.parseInt(splits[0]);

            long epochTime = Long.parseLong(splits[1]);

            float gpsLong = Float.parseFloat(splits[2]);

            float gpsLat = Float.parseFloat(splits[3]);

            float netLong = Float.parseFloat(splits[4]);

            float netLat = Float.parseFloat(splits[5]);

            int cid = Integer.parseInt(splits[6]);
            mAllCids.add(cid);

            int lac = Integer.parseInt(splits[7]);

            int networkType = Integer.parseInt(splits[8]);

            int signalStr = Integer.parseInt(splits[9]);

            Date dateTime = new Date(epochTime);

            long duration = epochTime - mLastEpochTime;

            mLastEpochTime = epochTime;
            
            CidStats thisCInfo = mCidStats.get(cid);
            
            // This is here really just for the first case of null cid
            if (thisCInfo == null) {
                thisCInfo = new CidStats();
                thisCInfo.setCid(cid);
                mCidStats.put(cid, thisCInfo);                    
            }
            
            // TODO: Check if the cell is in an activity already, if yes, then do not add to unprocessed list
            if (thisCInfo.getActivity() == null){                
                mUnprocessedCids.add(cid);
            }
              
            CidStats lastCInfo = mCidStats.get(mLastCid);
            if (lastCInfo != null){
                //  Add to the total duration spent at the last cell
                lastCInfo.addDuration(duration);
            }

            if (mLastCid != cid) {
                thisCInfo.incTransitonCount(mLastCid);
                if (lastCInfo != null)
                    lastCInfo.incTransitonCount(cid);
                
                // We are also interested in the GPS coordinates if possible
                LatLong latLong = new LatLong(gpsLat, gpsLong);
                thisCInfo.addLocation(latLong);
            }

            mLastCid = cid;

            // long now = System.currentTimeMillis();
            // if (now - mLastMergeTime > MERGE_THRESHOLD){
            // merge();
            // }

            if (epochTime - mLastMergeTime > MERGE_THRESHOLD) {
                mLastMergeTime = epochTime;
                doMerge(epochTime);
            }

            if (epochTime - mLastMajorMerge > MAJOR_MERGE_THRESHOLD) {
                mLastMajorMerge = epochTime;
                doMajorMerge(epochTime);
            }

        } catch (NumberFormatException exp) {
            System.err.print("Failed to parse the line " + line);
        }

    }

    /**
     * Major merge, look back in the total duration and transition count of past
     * 24 hours
     * 
     * @param epochTime
     */
    private static void doMajorMerge(long epochTime) {

        System.out.println("Before major merge");
        printActivities();
        
        HashMap<String, String> mergeMap = new HashMap<String, String>(); // Which
                                                                  // activity
                                                                  // has already
                                                                  // mapped to
                                                                  // which
        HashMap<String, ActivityStats> postMergeActivities = new HashMap<String, ActivityStats>();

        // Go through activities and merge them based on transition counts of
        // the CIDs
        Set<String> activityIdSet = mActivities.keySet();
        java.util.Iterator<String> iterator = activityIdSet.iterator();
        while (iterator.hasNext()) {
            ActivityStats activity = mActivities.get(iterator.next());
            HashSet<Integer> cids = activity.getCids();
            HashMap<String, ActivityStats> activitiesToMerge = new HashMap<String, ActivityStats>();
            java.util.Iterator<Integer> cidIter = cids.iterator();
            
            // Go through all cids of this activity
            while (cidIter.hasNext()) {
                int cid = cidIter.next();
                CidStats cidStats = mCidStats.get(cid);
                if (cidStats == null) {
                    continue;
                }
                HashMap<Integer, Integer> transitions = cidStats.getTotalTransitionCounts();
                Set<Integer> connectedCids = transitions.keySet();
                java.util.Iterator<Integer> iter = connectedCids.iterator();

                ActivityStats nActivity = null;
                int higestCount = 0;

                // Check the cid's most visited neighbours
                while (iter.hasNext()) {
                    int nCid = iter.next();
                    
                    // Skip the cells this cell already connected to
                    if (cidStats.getActivity().getCids().contains(nCid)){
                        continue;
                    }
                    
                    int nCount = transitions.get(nCid);
                    if (nCount > TRANSITION_TOTAL_COUNT_THRESHOLD) {
                        CidStats nCstats = mCidStats.get(nCid);
                        if (nCstats != null) {

                            // Merge with the cell that transition
                            // to the most and has an activity
                            nActivity = nCstats.getActivity();
                            higestCount = nCount;

                        }// end if nCstats != null
                    } // end if count > threshold
                } // end if iter.hasNext()

                if (nActivity != null) {
                    activitiesToMerge.put(nActivity.getId(), nActivity);
                }
            }// end loop cids

            // TODO: What if cells connect to different activities? They
            // probably shouldn't be in the sam activity to start off with!
            if (activitiesToMerge.size() > 0) {
                
                // In case if the current activity has already been mapped
                String actId = activity.getId();               
                while (mergeMap.containsKey(actId)){
                    actId = mergeMap.get(actId);
                    activity = postMergeActivities.get(actId);
                }
                
                activitiesToMerge.put(actId, activity);
                // If we merge here, the loop will be invalid, so we need to
                // delay the merge action                
                ActivityStats newActivity = doMergeActivities(activitiesToMerge, mergeMap,
                        postMergeActivities);

                // Remove merged activities from the new list                
                java.util.Iterator<String> removeKeyIter = activitiesToMerge.keySet().iterator();
                while (removeKeyIter.hasNext()) {
                    postMergeActivities.remove(removeKeyIter.next());
                }

                // put the new one in
                postMergeActivities.put(newActivity.getId(), newActivity);
            } else {
                // No merge for this activity, keep it as it is
                //  If this has not already been merged
                Set<String> mergedCids = mergeMap.keySet();
                if (!mergedCids.contains(activity.getId())){
                    postMergeActivities.put(activity.getId(), activity);
                }
            }
        } // end loop mActivities

        mActivities = new HashMap<String, ActivityStats>(postMergeActivities);

        // Go through the set of set of activities tagged, then merge them

        // Go through the unprocessed CIDs and merge with cells with known
        // activities that transit to them a lot
        HashSet<Integer> unProcessedCids = new HashSet<Integer>();
        java.util.Iterator<Integer> unprocCidIter = mUnprocessedCids.iterator();
        while (unprocCidIter.hasNext()) {
            int uCid = unprocCidIter.next();
            if (uCid == -1){
                continue;
            }
            
            CidStats cStats = mCidStats.get(uCid);
            // Check if the cell actually belongs to a larger group

            HashMap<Integer, Integer> transitionCounts = cStats.getTotalTransitionCounts();
            Set<Integer> connectedCids = transitionCounts.keySet();
            java.util.Iterator<Integer> iter = connectedCids.iterator();

            ActivityStats nActivity = null;
            int higestCount = 0;

            while (iter.hasNext()) {
                int cid = iter.next();
                int count = transitionCounts.get(cid);

                if (count > TRANSITION_TOTAL_COUNT_THRESHOLD) {
                    CidStats nCstats = mCidStats.get(cid);

                    // Maybe the device hasn't yet switched to its
                    // neighbouring cells, then don't care
                    if (nCstats != null) {

                        if (count > higestCount) {
                            nActivity = nCstats.getActivity();
                            higestCount = count;
                        }

                    }// end if nCstats != null
                } // end if count > threshold
            }// end while iter.next

            if (nActivity != null) {
                cStats.setActivity(nActivity);
                nActivity.addCid(cStats.getCid());
            } else {
                unProcessedCids.add(cStats.getCid());
            }
        }// end for mUnprocessed
        if (unProcessedCids.size() != mUnprocessedCids.size()) {
            mUnprocessedCids = unProcessedCids;
        }
        
        // DEBUG: print out the activities
        System.out.println("Major merge performed");
        printActivities();
        
        System.out.println("Unprocessed cids:");
        
        unprocCidIter = mUnprocessedCids.iterator();
        while(unprocCidIter.hasNext()) {
            System.out.print(unprocCidIter.next() + ";");
        }
        
        // Reset all stats after 24 hours
        Set<Integer> keySet = mCidStats.keySet();
        java.util.Iterator<Integer> statIterator = keySet.iterator();
        while (statIterator.hasNext()){
            Integer cid = statIterator.next();
            CidStats cStats = mCidStats.get(cid);
            cStats.resetTotalDuration();
            cStats.resetTotalTransitionCounts();
        }
    }

    private static void printActivities() {        
        Set<String> activities = mActivities.keySet();
        java.util.Iterator<String> iterator2 = activities.iterator();
        while (iterator2.hasNext()) {
            String id = iterator2.next();
            ActivityStats activityStats = mActivities.get(id);
            System.out.println(" Activity <" + id + ">");
            HashSet<Integer> cids = activityStats.getCids();
            System.out.println("  Cids:" + cids);
        }
    }

    private static ActivityStats doMergeActivities(HashMap<String, ActivityStats> activitiesToMerge,
            HashMap<String, String> mergeMap, HashMap<String, ActivityStats> postMergeActivities) {
        Set<String> actIds = activitiesToMerge.keySet();
        java.util.Iterator<String> iter = actIds.iterator();

        // Now perform merge
        ActivityStats newActivity = new ActivityStats();
        ArrayList<Integer> totalCids = new ArrayList<Integer>();

        while (iter.hasNext()) {
            ActivityStats actStats = activitiesToMerge.get(iter.next());

            HashSet<Integer> cids = actStats.getCids();
            totalCids.addAll(cids); // Need to check if it is possible to have
                                    // repeat cids

            mergeMap.put(actStats.getId(), newActivity.getId());
        }

        newActivity.addCid(totalCids);
        
        // Repoint all cids to this new activity
        HashSet<Integer> cids = newActivity.getCids();
        java.util.Iterator<Integer> cidIterator = cids.iterator();
        while(cidIterator.hasNext()){
            CidStats cidStats = mCidStats.get(cidIterator.next());
            if (cidStats != null){
                cidStats.setActivity(newActivity);
            }
        }            
        return newActivity;
    }

    private static void doMerge(long mergeTime) {

        HashSet<Integer> cidsStillUnProcessed = new HashSet<Integer>();

        java.util.Iterator<Integer> unprocInter = mUnprocessedCids.iterator();
        while (unprocInter.hasNext()) {
            Integer unprocCid = unprocInter.next();
            if (unprocCid == -1){
                continue;
            }
            CidStats cStats = mCidStats.get(unprocCid);
            long duration = cStats.getDuration();
//            long totalDuration = cStats.getTotalDuration();
//
//            String durationReadable = String.format(
//                    "%d min, %d sec",
//                    TimeUnit.MILLISECONDS.toMinutes(totalDuration),
//                    TimeUnit.MILLISECONDS.toSeconds(totalDuration)
//                            - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS
//                                    .toMinutes(totalDuration)));
//
//            System.out.println("\t" + cStats.getCid() + ">" + durationReadable);

            if (duration > DURATION_THRESHOLD) {

                // Probably a potential activity, if the over all connection
                // time is long

                // 1. We want to check the neighbouring cells and the transition
                // counts
                HashMap<Integer, Integer> transitionCounts = cStats.getTransitionCounts();
                Set<Integer> connectedCids = transitionCounts.keySet();
                java.util.Iterator<Integer> iter = connectedCids.iterator();

                ActivityStats nActivity = null;
                int higestCount = 0;

                while (iter.hasNext()) {
                    int cid = iter.next();
                    int count = transitionCounts.get(cid);

                    if (count > TRANSITION_COUNT_THRESHOLD) {
                        CidStats nCstats = mCidStats.get(cid);

                        // Maybe the device hasn't yet switched to its
                        // neighbouring cells, then don't care
                        if (nCstats != null) {

                            // we may want to see if the note also transit back
                            // enough times

                            // We probably want to also know what's the
                            // average time spent in the neighbouring
                            // cells before transit back
                            // For now we just take the activity of the
                            // neighbouring cell
                            if (count > higestCount) {
                                nActivity = nCstats.getActivity();
                                higestCount = count;
                            }
                        }
                    }
                }

                if (nActivity != null) {
                    cStats.setActivity(nActivity);
                    nActivity.addCid(cStats.getCid());
                    continue; // To the next unprocessed cid
                }

                // if neighbours don't have activity, create one
                ActivityStats newActivity = new ActivityStats();
                newActivity.addCid(cStats.getCid());
                cStats.setActivity(newActivity);
                mActivities.put(newActivity.getId(), newActivity);
            } // END-IF： Activity connection duration is long
            else {
                // If the total connection duration to the cell tower within the
                // detection period is lower than threshold
                cidsStillUnProcessed.add(unprocCid);
            }
        } // END-LOOP: unprocessedCids

        mUnprocessedCids = cidsStillUnProcessed;
        
//        System.out.println("Minor merge performed:");
//        printActivities();
        
        // Reset stats of all cids
        Set<Integer> keySet = mCidStats.keySet();
        java.util.Iterator<Integer> iterator = keySet.iterator();
        while (iterator.hasNext()){
            Integer cid = iterator.next();
            CidStats cStats = mCidStats.get(cid);
            cStats.resetDuration();
            cStats.resetTransitionCounts();
        }

    }   
}
