import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class CidStats {

    private Integer mCid;
    private long mDuration;
    private ActivityStats mActivity;
    private HashMap<Integer, Integer> mTransitionCountMap = new HashMap<Integer, Integer>();
    private long mTotalDuration;
    private HashMap<Integer, Integer> mTotalTransitionCountMap = new HashMap<Integer, Integer>();
    private ArrayList<LatLong> mLocations = new ArrayList<LatLong>();

    public void setCid(Integer cid) {
        mCid = cid;
    }

    public void setDuration(long duration) {    
        mDuration = duration;
    }

    public void addDuration(long duration) {
        mDuration += duration;
    }

    public long getDuration() {
        return mDuration;
    }

    public ActivityStats getActivity() {
        return mActivity;
    }
    
    public void setActivity(ActivityStats activity){
        mActivity = activity;
    }

    public int getCid() {
        return mCid;
    }

    public void incTransitonCount(int cid) {
        Integer count = mTransitionCountMap.get(cid);
        if (count ==null){
            count = 0;            
        }
        count++;
        mTransitionCountMap.put(cid, count);            
    }

    public HashMap<Integer, Integer> getTransitionCounts() {
        return mTransitionCountMap;
        
    }

    public HashMap<Integer, Integer> getTotalTransitionCounts(){
        return mTotalTransitionCountMap;
    }
    public void resetTransitionCounts() {
           Iterator it = mTransitionCountMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pairs = (Map.Entry)it.next();
                int cid = (Integer) pairs.getKey();
                int count = (Integer) pairs.getValue();
                Integer curCount = mTotalTransitionCountMap.get(cid);
                if (curCount == null){
                    curCount = 0;
                }
                curCount+= count;
                mTotalTransitionCountMap.put(cid, curCount);
                
                // Reset value
                pairs.setValue(0);
            }
    }

    public void resetDuration() {
        mTotalDuration += mDuration;
        mDuration = 0;
    }

    public void resetTotalDuration() {
        mTotalDuration = 0;
    }

    public void resetTotalTransitionCounts() {
        mTotalTransitionCountMap = new HashMap<Integer, Integer>(); 
    }

    public ArrayList<LatLong> getLocations(){
        return mLocations;
    }
    
    public void addLocation(LatLong latLong) {
        if (latLong.latitude == -999999.0 || latLong.latitude == 0.0){
            return;
        }
        
        boolean newLocation = false;
        int diffCounter = 0;
        // Calculate the distance, if very different (500 meters) to all places recorded, note it down        
        for (int i=0; i< mLocations.size(); i++){
            double distance  = distanceBetween(mLocations.get(i), latLong);
            if (distance > 500){
                newLocation = true;
                diffCounter++;
            }
        }
        if (mLocations.size() == 0){
            mLocations.add(latLong);
        }
        if (newLocation && diffCounter > mLocations.size()/2){
            mLocations.add(latLong);
        }
    }

    private double distanceBetween(LatLong startpoint, LatLong endpoint) {
        double earthRadius = 3958.75;
        double dLat = Math.toRadians(endpoint.latitude-startpoint.latitude);
        double dLng = Math.toRadians(endpoint.longitude-startpoint.longitude);
        double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                   Math.cos(Math.toRadians(startpoint.latitude)) * Math.cos(Math.toRadians(endpoint.latitude)) *
                   Math.sin(dLng/2) * Math.sin(dLng/2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        double dist = earthRadius * c;

        int meterConversion = 1609;
        
        return meterConversion*dist;
    }

}
