
public class LatLong {
    public float latitude;
    public float longitude;
    public LatLong(Float gpsLat, Float gpsLong) {
        latitude = gpsLat;
        longitude = gpsLong;
    }
            
    public String toString() {
        String result = latitude + "," + longitude;
        return result;
    }
}
