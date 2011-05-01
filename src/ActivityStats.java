import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;


public class ActivityStats {
    
    private String mId;
    
    public ActivityStats(){
        UUID uuid = UUID.randomUUID();
        mId = uuid.toString();
    }
    
    HashSet<Integer> mCids = new HashSet<Integer>();
    
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
}