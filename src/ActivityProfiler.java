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
import java.util.Map.Entry;
import java.util.Set;

public class ActivityProfiler {

    private static final long DURATION_THRESHOLD = 10 * 60 * 1000; 

    private static final long MERGE_THRESHOLD = 4 * 60 * 60 * 1000; 

    private static final int TRANSITION_COUNT_THRESHOLD = 10; 
    
    private static final int TRANSITION_TOTAL_COUNT_THRESHOLD = 20; 

    private static final long MAJOR_MERGE_THRESHOLD = 24 * 60 * 60 * 1000; 
    
    private static final long PING_PONG_THRESHOLD = 2 * 60 * 1000; 

    private static final int CIR_BUF_SIZE = 2;
    
    private static final long MAJOR_DURATION_THRESHOLD = 30 * 60 * 1000;

    private void printActivities(boolean noTemporal) {

        // Print CID duration
        // long totalDuration = cStats.getTotalDuration();
        //
        // String durationReadable = String.format(
        // "%d min, %d sec",
        // TimeUnit.MILLISECONDS.toMinutes(totalDuration),
        // TimeUnit.MILLISECONDS.toSeconds(totalDuration)
        // - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS
        // .toMinutes(totalDuration)));
        //
        // System.out.println("\t" + cStats.getCid() + ">" + durationReadable);

        Set<String> activities = mActivities.keySet();
        java.util.Iterator<String> iterator2 = activities.iterator();
        while (iterator2.hasNext()) {
            String id = iterator2.next();
            ActivityStats activityStats = mActivities.get(id);
            
            if (noTemporal && activityStats.isTemporal()){
                continue;
            }
            
            System.out.println(" Activity <" + id + ">");
            System.out.println("Temporal:" + activityStats.isTemporal());
            HashSet<Integer> cids = activityStats.getCids();
            System.out.println("Cids:");
            
            StringBuffer strBuf = new StringBuffer();
            for (Iterator<Integer> cidIter = cids.iterator(); cidIter.hasNext();){
                int cid = cidIter.next();
                strBuf.append(cid);
                strBuf.append("\nTransits:\n");
                CidStats cStats = mCidStats.get(cid);
               
                for ( Iterator<Entry<Integer, Integer>> transitIter = cStats.getDebugTransitionCounts().entrySet().iterator();
                transitIter.hasNext();){
                    Entry<Integer, Integer> transit = transitIter.next();
                    strBuf.append(transit.getKey() + ">>"+transit.getValue());
                    if (getActivity(mCidStats.get(transit.getKey())) == activityStats){
                        strBuf.append("*");
                    }
                    strBuf.append("\n");
                }
                strBuf.append("\n");
            }
            System.out.println(strBuf.toString());
        }
    }

    private void printActivitiesForMatLab(boolean noTemporal) {
        int counter = 1;
        Iterator<String> iterator = mActivities.keySet().iterator();
        System.out.println("ActId\tCID\tlatitude\tlongitude");
        while (iterator.hasNext()) {
            StringBuffer strBuf = new StringBuffer();
            String actId = iterator.next();
            ActivityStats actStats = mActivities.get(actId);
            
            if (noTemporal && actStats.isTemporal()){
                continue;
            }
            
            Iterator<Integer> cidIter = actStats.mCids.iterator();
            while (cidIter.hasNext()) {
                int cid = cidIter.next();
                CidStats cStat = mCidStats.get(cid);
                
                // For narseo's data
                if (cStat.getLocations().size()>0 && cStat.getLocations().get(0).longitude < 0){
                    continue;
                }
//                
                strBuf.append(produceMatlabLocList(counter, cid, cStat.getLocations()));
            }
            // System.out.println("<" + actId + ">");
            System.out.print(strBuf.toString());
            counter++;
        }
    }

    private static String produceMatlabLocList(int actId, int cid, ArrayList<LatLong> locList) {
        StringBuffer strBuf = new StringBuffer();
        Iterator<LatLong> iterator = locList.iterator();
        while (iterator.hasNext()) {
            LatLong latLong = iterator.next();
            strBuf.append(actId);
            strBuf.append("\t");
            strBuf.append(cid);
            strBuf.append("\t");
            strBuf.append(latLong.latitude);
            strBuf.append("\t");
            strBuf.append(latLong.longitude);
            strBuf.append("\n");
        }
        return strBuf.toString();
    }

    private void printActivitiesLocations(boolean noTemporal) {
        System.out.println("Activities locations:");
        Iterator<String> iterator = mActivities.keySet().iterator();
        while (iterator.hasNext()) {
            StringBuffer strBuf = new StringBuffer();
            String actId = iterator.next();
            ActivityStats actStats = mActivities.get(actId);
            
            if (noTemporal && actStats.isTemporal()){
                continue;
            }
            
            Iterator<Integer> cidIter = actStats.mCids.iterator();
            while (cidIter.hasNext()) {
                int cid = cidIter.next();
                CidStats cStat = mCidStats.get(cid);                
                strBuf.append(prettyPrintLocList(actStats.getId(), cid, cStat.getLocations()));
            }
            System.out.println("<" + actId + ">");
            System.out.println(actStats.mCids.toString());
            System.out.println(strBuf.toString());
        }
    }

    private String prettyPrintLocList(String actId, Integer cid, ArrayList<LatLong> locList) {
        StringBuffer strBuf = new StringBuffer();
        Iterator<LatLong> iterator = locList.iterator();
        while (iterator.hasNext()) {
            LatLong latLong = iterator.next();
            strBuf.append(actId);
            strBuf.append("\t");
            strBuf.append(cid);
            strBuf.append("\t");
            strBuf.append(latLong.latitude);
            strBuf.append("\t");
            strBuf.append(latLong.longitude);
            strBuf.append("\n");
        }
        return strBuf.toString();
    }

    private void printCidLocations() {
        System.out.println("Cid locations:");
        Iterator<Integer> iterator = mCidStats.keySet().iterator();
        while (iterator.hasNext()) {
            int cid = iterator.next();
            CidStats cStats = mCidStats.get(cid);
            System.out.println("<" + cid + ">");
            System.out.println(prettyPrintLocList("", cid, cStats.getLocations()));
        }
    }

    private HashMap<Integer, CidStats> mCidStats = new HashMap<Integer, CidStats>();

    private final static int INITIAL_CID = -2;

    private int mLastCid = INITIAL_CID;

    private long mLastEpochTime = 1302079027648L;

    private long mLastMergeTime;

    private HashSet<Integer> mUnprocessedCids = new HashSet<Integer>();

    private long mLastMajorMerge;

    private HashMap<String, ActivityStats> mActivities = new HashMap<String, ActivityStats>();

    private ActivityStats mCurrentActivity;

    private HashMap<String, ActivityStats> mTemporalActivities = new HashMap<String, ActivityStats>();

    int[] visitedCidBuffer = new int[CIR_BUF_SIZE];

    long[] connectDurationBuffer = new long[CIR_BUF_SIZE];

    /**
     * A new CID has been detected TODO: Think about how this will work in our
     * mobile system. Are all logs associated with an activity ID? What about
     * the events where activties are merged, do we need to merge all logs?
     * 
     * @param cid
     * @param epochTime
     * @param gpsLat
     * @param gpsLong
     */
    public void newCidEvent(int cid, long epochTime, Float gpsLat, Float gpsLong) {

        long duration = epochTime - mLastEpochTime;

        // TODO: Eliminate ping-pong effects. Have a buffer of previous N
        // transitions. If cid repeats in the last N transitions
        // within a time limit, then merge the activities

        // Update the circular buffer
        for (int i = 0; i < CIR_BUF_SIZE - 1; i++) {
            visitedCidBuffer[i] = visitedCidBuffer[i + 1];
            connectDurationBuffer[i] = connectDurationBuffer[i + 1];
        }
        visitedCidBuffer[CIR_BUF_SIZE - 1] = mLastCid;
        connectDurationBuffer[CIR_BUF_SIZE - 1] = duration;

//        if (epochTime == 1302102020768L) {
//            System.out.print(true);
//        }
        long elapsedTime = 0;
        for (int i = CIR_BUF_SIZE - 1; i > -1; i--) {
            if (visitedCidBuffer[i] == cid) {
                CidStats cidStats = mCidStats.get(cid);

                HashMap<String, ActivityStats> activitiesToMerge = new HashMap<String, ActivityStats>();
                for (int j = i; j < CIR_BUF_SIZE; j++) {
                    elapsedTime += connectDurationBuffer[j];
                    ActivityStats pastActivity = getActivity(mCidStats.get(visitedCidBuffer[j]));

                    // If the activities are the same, skip
                    if (cidStats.getActivity() == pastActivity) {
                        continue;
                    }

                    activitiesToMerge.put(pastActivity.getId(), pastActivity);
                }

                if (activitiesToMerge.size() > 99 && elapsedTime < PING_PONG_THRESHOLD) {
                    activitiesToMerge.put(cidStats.getActivity().getId(), cidStats.getActivity());
                    HashMap<String, String> mergeMap = new HashMap<String, String>();
                    // Merge all activities inbetween, then set the current
                    // activity as the merged one
                    // TODO: in our mobile system, are we expecting the
                    // summaries to be merged too?
                    ActivityStats newActivity = doMergeActivities(activitiesToMerge, mergeMap);
                    //
//                    System.out.println("Ping pong at" + cid + " at time " + epochTime);
//                    printActivities(false);

                    java.util.Iterator<String> removeKeyIter = activitiesToMerge.keySet()
                            .iterator();

                    // TODO: fix bug. the activity may not be in mActivities
                    while (removeKeyIter.hasNext()) {
                        String removeKey = removeKeyIter.next();
                        mTemporalActivities.remove(removeKey);
                        mActivities.remove(removeKey);
                    }

                    // put the new one in
                    mActivities.put(newActivity.getId(), newActivity);
                    if (newActivity.isTemporal()) {
                        mTemporalActivities.put(newActivity.getId(), newActivity);
                    }

//                    System.out.println("Merged!");
//                    printActivities(false);
                }
                break;
            }
        }

        //
        mLastEpochTime = epochTime;

        CidStats thisCInfo = mCidStats.get(cid);

        if (thisCInfo == null) {
            thisCInfo = new CidStats();
            thisCInfo.setCid(cid);
            mCidStats.put(cid, thisCInfo);
        }

        // Increase transition count associated with the previous cid
        if (mLastCid != INITIAL_CID) {
            thisCInfo.incTransitonCount(mLastCid);
        }

        if (mCurrentActivity != null) {
            mCurrentActivity.disconnected(epochTime);
        }

        // Check if this CID is in an activity already
        ActivityStats activity = getActivity(thisCInfo);
        if (thisCInfo.getActivity() == null) {
            // If no activity, create a temporal one
            activity = new ActivityStats();
            activity.addCid(cid);
            activity.connected(epochTime);
            activity.setTemporal(true);
            mUnprocessedCids.add(cid);
            mTemporalActivities.put(activity.getId(), activity);
            mActivities.put(activity.getId(), activity);
            thisCInfo.setActivity(activity);
        } else {
            mCurrentActivity = activity;
        }

        CidStats lastCInfo = mCidStats.get(mLastCid);

        // Add to the total duration spent at the last cell, and the transition
        if (lastCInfo != null) {
            lastCInfo.addDuration(duration);
            lastCInfo.incTransitonCount(cid);
        }

        // We are also interested in the GPS coordinates if possible
        if (gpsLat != null && gpsLong != null) {
            LatLong latLong = new LatLong(gpsLat, gpsLong);
            thisCInfo.addLocation(latLong);
        }

        // Perform minor merge
        if (epochTime - mLastMergeTime > MERGE_THRESHOLD) {
            mLastMergeTime = epochTime;
            doMerge(epochTime);
        }

        // Perform major merge
         if (epochTime - mLastMajorMerge > MAJOR_MERGE_THRESHOLD) {
         mLastMajorMerge = epochTime;
         doMajorMerge(epochTime);
         }

        mLastCid = cid;
    }

    public ActivityStats getActivity(CidStats cidStats) {
        return cidStats.getActivity();
    }

    /**
     * Minor merge. Go through all temporal activities. If the total connection
     * time is long enough, recognise it as separate activity. If the
     * transitions from the including CIDs to other CIDs are very frequent,
     * merge to that activity.
     * 
     * @param mergeTime
     */
    private void doMerge(long mergeTime) {

        Set<String> tempActsToRemove = new HashSet<String>();
        Set<ActivityStats> tempActsToAdd = new HashSet<ActivityStats>();

        Iterator<Entry<String, ActivityStats>> tempActIter = mTemporalActivities.entrySet()
                .iterator();

        while (tempActIter.hasNext()) {
            Entry<String, ActivityStats> tempActEntry = tempActIter.next();
            ActivityStats tempAct = tempActEntry.getValue();

            long duration = tempAct.getDuration();

            if (duration > DURATION_THRESHOLD) {
                tempAct.setTemporal(false);
                tempActIter.remove();
                mActivities.put(tempActEntry.getKey(), tempActEntry.getValue());
            }

            // else
            {
                // Get total transit map
                HashMap<Integer, Integer> totalTransitions = new HashMap<Integer, Integer>();
                HashSet<Integer> cids = tempAct.getCids();
                Iterator<Integer> cidIter = cids.iterator();
                while (cidIter.hasNext()) {
                    int cid = cidIter.next();
                    CidStats cStats = mCidStats.get(cid);

                    HashMap<Integer, Integer> transitionCounts = cStats.getTransitionCounts();
                    Iterator<Entry<Integer, Integer>> transitionIter = transitionCounts.entrySet()
                            .iterator();
                    while (transitionIter.hasNext()) {
                        Entry<Integer, Integer> transition = transitionIter.next();
                        totalTransitions.put(transition.getKey(), transition.getValue());
                    }
                }

                Iterator<Entry<Integer, Integer>> iter = totalTransitions.entrySet().iterator();
                ActivityStats nActivity = null;
                int higestCount = 0;

                while (iter.hasNext()) {
                    Entry<Integer, Integer> transitEntry = iter.next();
                    int transitCid = transitEntry.getKey();

                    // Skip local transits
                    if (tempAct.getCids().contains(transitCid)) {
                        continue;
                    }

                    int count = transitEntry.getValue();

                    // If this activity transit to another one very often
                    if (count > TRANSITION_COUNT_THRESHOLD) {
                        CidStats nCstats = mCidStats.get(transitCid);

                        if (nCstats != null) {
                            if (count > higestCount) {
                                nActivity = nCstats.getActivity();
                                higestCount = count;
                            }
                        }
                    }
                }

                if (nActivity != null) {
                    HashMap<String, ActivityStats> activitiesToMerge = new HashMap<String, ActivityStats>();
                    activitiesToMerge.put(nActivity.getId(), nActivity);
                    activitiesToMerge.put(tempAct.getId(), tempAct);
                    HashMap<String, String> mergeMap = new HashMap<String, String>();
                    ActivityStats newActivity = doMergeActivities(activitiesToMerge, mergeMap);

                    // Remove the previous activities from activities list, add
                    // the new one
                    Iterator<Entry<String, ActivityStats>> mergedActIter = activitiesToMerge
                            .entrySet().iterator();
                    while (mergedActIter.hasNext()) {
                        ActivityStats mergedAct = mergedActIter.next().getValue();

                        mActivities.remove(mergedAct.getId());

                        tempActsToAdd.remove(mergedAct);
                        if (mergedAct.isTemporal()) {
                            // Need to mark for later remove
                            tempActsToRemove.add(mergedAct.getId());
                        }
                    }

                    mActivities.put(newActivity.getId(), newActivity);
                    if (newActivity.isTemporal()) {
                        tempActsToAdd.add(newActivity);
                    }

                    continue; // To the next temporal activity
                }
            }
        } // END-LOOP: temporal activities

        if (tempActsToRemove.size() > 0) {
            Iterator<String> rmActIter = tempActsToRemove.iterator();
            while (rmActIter.hasNext()) {
                String actToRemove = rmActIter.next();
                mTemporalActivities.remove(actToRemove);
            }
        }

        // System.out.println("Minor merge performed:");
        // printActivities();

        Iterator<Entry<String, ActivityStats>> actIter = mActivities.entrySet().iterator();
        while (actIter.hasNext()) {
            ActivityStats act = actIter.next().getValue();
            act.resetAfterMinorMerge();
        }

        // Reset stats of all cids
        Iterator<Entry<Integer, CidStats>> cidIter = mCidStats.entrySet().iterator();
        while (cidIter.hasNext()) {
            CidStats cid = cidIter.next().getValue();
            cid.resetDuration();
            cid.resetTransitionCounts();
        }
    }

    /**
     * Major merge. First go through all temporal activities, again look at
     * their total connection time, and transition counts. Then go through all
     * activities, merge those that connect to each other very often.
     * 
     * @param epochTime
     */
    private void doMajorMerge(long epochTime) {

        // System.out.println("Before major merge");
        // printActivities();
        Set<String> tempActsToRemove = new HashSet<String>();
        Set<ActivityStats> tempActsToAdd = new HashSet<ActivityStats>();

        Iterator<Entry<String, ActivityStats>> tempActIter = mTemporalActivities.entrySet()
                .iterator();

        while (tempActIter.hasNext()) {
            Entry<String, ActivityStats> tempActEntry = tempActIter.next();
            ActivityStats tempAct = tempActEntry.getValue();

            long duration = tempAct.getDuration();

            // TODO: use a different threshold
            if (duration > MAJOR_DURATION_THRESHOLD) {
                tempAct.setTemporal(false);
                tempActIter.remove();
                mActivities.put(tempActEntry.getKey(), tempActEntry.getValue());
            }
        }
        
        // Go through all activities and merge highly connected ones
        HashMap<String, String> mergeMap = new HashMap<String, String>();
        HashMap<String,ActivityStats> actsToRemove = new HashMap<String,ActivityStats>();
        HashMap<String,ActivityStats> actsToAdd = new HashMap<String,ActivityStats>();
        for (Iterator<Entry<String, ActivityStats>> actIter = mActivities.entrySet().iterator(); actIter.hasNext();){
            ActivityStats nActivity = null;
            int highestCount = 0;
            
            ActivityStats actStats = actIter.next().getValue();
            for (Iterator<Integer> cidIter = actStats.getCids().iterator();cidIter.hasNext();){
                int cid = cidIter.next();
//                if (cid == 65373){
//                    System.out.println("here");
//                }
                CidStats cidStats = mCidStats.get(cid);
                HashMap<Integer, Integer> totalTransitionCounts = cidStats.getTotalTransitionCounts();                
                for (Iterator<Entry<Integer, Integer>> transitionIter = totalTransitionCounts.entrySet().iterator(); transitionIter.hasNext();){
                    Entry<Integer, Integer> transition = transitionIter.next();
                    int neighbourCid = transition.getKey();
                    // Skip cid that is already in the activity
                    if (actStats.getCids().contains(neighbourCid)){
                        continue;
                    }
                    int transitCount = transition.getValue();
                    
                    // TODO: add other checks apart from a static threshold
                    if (transitCount > TRANSITION_TOTAL_COUNT_THRESHOLD && transitCount > highestCount){
                        nActivity = mCidStats.get(neighbourCid).getActivity();                        
                        highestCount = transitCount;
                    }
                }
            }
            
            if (highestCount > 0 && nActivity != null){
                HashMap<String, ActivityStats> activitiesToMerge = new HashMap<String, ActivityStats>();
                activitiesToMerge.put(nActivity.getId(), nActivity);
                activitiesToMerge.put(actStats.getId(), actStats);
                ActivityStats newActivity = doMergeActivities(activitiesToMerge, mergeMap);                
                for (Iterator<Entry<String, ActivityStats>> iterator = activitiesToMerge.entrySet().iterator();iterator.hasNext();){
                    Entry<String, ActivityStats> actEntry = iterator.next();
                    String actId = actEntry.getKey();
                    ActivityStats actToRemove = actEntry.getValue();
                    actsToAdd.remove(actToRemove);
                    if (actToRemove.isTemporal()){
                        mTemporalActivities.remove(actId);
                    }
                    actsToRemove.put(actId, actToRemove);
                }
                actsToAdd.put(newActivity.getId(), newActivity);                
            }
        } // End loop mActivities
        
        if (actsToAdd.size() > 0){
            mActivities.putAll(actsToAdd);
        }
        
        if (actsToRemove.size() > 0){
            for (Iterator<String> iterator = actsToRemove.keySet().iterator();iterator.hasNext();){               
                mActivities.remove(iterator.next());
            }
        }
        
    
        // System.out.println("Major merge performed");
        // printActivities();
        
        
        // Reset all stats after 24 hours
        for (Iterator<Entry<String, ActivityStats>> actIter = mActivities.entrySet().iterator();actIter.hasNext();){
            ActivityStats actStat = actIter.next().getValue();
            actStat.resetAfterMajorMerge();
        }
        
        Set<Integer> keySet = mCidStats.keySet();
        java.util.Iterator<Integer> statIterator = keySet.iterator();
        while (statIterator.hasNext()) {
            Integer cid = statIterator.next();
            CidStats cStats = mCidStats.get(cid);
            cStats.resetTotalDuration();
            cStats.resetTotalTransitionCounts();
        }
    }

    private ActivityStats doMergeActivities(HashMap<String, ActivityStats> activitiesToMerge,
            HashMap<String, String> mergeMap) {
        Set<String> actIds = activitiesToMerge.keySet();
        java.util.Iterator<String> iter = actIds.iterator();

        // Now perform merge
        ActivityStats newActivity = new ActivityStats();
        ArrayList<Integer> totalCids = new ArrayList<Integer>();
        boolean isTemporal = true;
        long duration = 0;

        while (iter.hasNext()) {
            ActivityStats actStats = activitiesToMerge.get(iter.next());

            HashSet<Integer> cids = actStats.getCids();
            totalCids.addAll(cids);
            isTemporal = isTemporal & actStats.isTemporal();
            duration += actStats.getDuration();

            mergeMap.put(actStats.getId(), newActivity.getId());
        }

        newActivity.addCid(totalCids);
        newActivity.setTemporal(isTemporal);
        newActivity.setDuration(duration);

        java.util.Iterator<Integer> cidIterator = newActivity.getCids().iterator();
        while (cidIterator.hasNext()) {
            CidStats cidStats = mCidStats.get(cidIterator.next());
            if (cidStats != null) {
                cidStats.setActivity(newActivity);
            }
        }
        return newActivity;
    }

    /**
     * Handles machine shutdown event
     * 
     * @param time
     */
    public void shutdown(long time) {
         printActivities(true);
         printActivitiesLocations(true);
//        printActivitiesForMatLab(true);
    }

    /**
     * Writes known activity states into file
     */
    public void writeToFile() {

    }

    /**
     * Restores activities from file at startup
     */
    public void restoreFromFile() {

    }
}
