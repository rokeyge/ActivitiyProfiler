import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;


public class ActivityStats {
    
    private static final long INVALID_TIME = -1;
    private String mId;
    
    public ActivityStats(){
        UUID uuid = UUID.randomUUID();
        mId = uuid.toString();
    }
    
    HashSet<Integer> mCids = new HashSet<Integer>();
    private long mStartTime;
    private long mConnectionDuration;
    private boolean mIsTemporal;
    private long mTotalDuration;
    
    public void addCid(int cid){
        mCids.add(cid);
    }

    public HashSet<Integer> getCids() {
        return mCids;
    }

    public String getId() {        
        return mId;
    }

    public void addCid(ArrayList<Integer> totalCids) {        
        mCids.addAll(totalCids);
    }

    public void connected(long epochTime) {
        mStartTime = epochTime;        
    }
    
    public void disconnected(long time) {
        if (mStartTime != INVALID_TIME){
            long duration = time - mStartTime;
            mConnectionDuration += duration;
            mStartTime = INVALID_TIME;
        }
    }

    public long getDuration() {
        return mConnectionDuration;
    }

    public void setTemporal(boolean b) {
        mIsTemporal = b;
    }

    public boolean isTemporal() {
        return mIsTemporal;
    }

    public void setDuration(long duration) {
        mConnectionDuration = duration;        
    }

    public void resetAfterMinorMerge() {
        mTotalDuration += mConnectionDuration;
        mConnectionDuration = 0;
    }
}