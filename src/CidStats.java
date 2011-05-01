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

}
