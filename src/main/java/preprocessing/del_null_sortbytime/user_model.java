package preprocessing.del_null_sortbytime;

import org.apache.hadoop.io.WritableComparable;
import org.apache.http.util.TextUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class user_model implements WritableComparable<user_model> {
    private long utc_ms;
    private int  msisdn;
    private int base_station;
    private double lati;
    private double longi;
    private int cluster;
    private boolean isVisit;
    private boolean isNoised;
    private String new_utc_ms;

    public user_model() {
    }

    public user_model(int msisdn, long utc_ms,  int base_station) {
        this.msisdn = msisdn;
        this.utc_ms = utc_ms;
        this.base_station = base_station;
    }

    public user_model(int utc_ms, int msisdn, int base_station, double longi, double lati) {
        this.utc_ms = utc_ms;
        this.msisdn = msisdn;
        this.base_station = base_station;
        this.longi = longi;
        this.lati = lati;

    }

    public user_model(int msisdn,long utc_ms, int base_station, double longi,  double lati,boolean isVisit,int cluster,  boolean isNoised) {
        this.utc_ms = utc_ms;
        this.msisdn = msisdn;
        this.base_station = base_station;
        this.lati = lati;
        this.longi = longi;
        this.cluster = 0;
        this.isVisit = false;
        this.isNoised = false;
    }

    public boolean isVisit() {
        return isVisit;
    }

    public boolean isNoised() {
        return isNoised;
    }

    public void setVisit(boolean visit) {
        isVisit = visit;
    }

    public void setNoised(boolean noised) {
        isNoised = noised;
    }

    public int getCluster() {
        return cluster;
    }

    public long getUtc_ms() {
        return utc_ms;
    }

    public int getMsisdn() {
        return msisdn;
    }

    public int getBase_station() {
        return base_station;
    }

    public double getLati() {
        return lati;
    }

    public double getLongi() {
        return longi;
    }

    public void setUtc_ms(long utc_ms) {
        this.utc_ms = utc_ms;
    }

    public void setMsisdn(int msisdn) {
        this.msisdn = msisdn;
    }

    public void setBase_station(int base_station) {
        this.base_station = base_station;
    }

    public void setLati(double lati) {
        this.lati = lati;
    }

    public void setLongi(double longi) {
        this.longi = longi;
    }

    public void setCluster(int cluster) {
        this.cluster = cluster;
    }

    public String toString(){
        return msisdn+","+utc_ms+","+base_station;
    }

    public String toStringCluster(){
        return msisdn+","+utc_ms+","+base_station+","+cluster;
    }

    public String toStringCluster_date(){
        new_utc_ms = timeTransform(utc_ms,"yyyy-MM-dd HH:mm:ss");
        return msisdn+","+new_utc_ms+","+base_station+","+cluster;
    }

    public String timeTransform(long utc,String formats ){
        if(TextUtils.isEmpty(formats)){
            formats = "yyyy-MM-dd HH:mm:ss";
        }
        String date_string = new SimpleDateFormat(formats, Locale.CHINA).format(new Date(utc));
        return date_string;
    }



    //重写根据经纬度计算距离
    public double getDistance(user_model o) {

        double long1 = this.longi;
        double lat1 = this.lati;
        double long2 = o.longi;
        double lat2 = o.lati;

        double a, b, R;
        R = 6378137; // 地球半径
        lat1 = lat1 * Math.PI / 180.0;
        lat2 = lat2 * Math.PI / 180.0;
        a = lat1 - lat2;
        b = (long1 - long2) * Math.PI / 180.0;
        double d;
        double sa2, sb2;
        sa2 = Math.sin(a / 2.0);
        sb2 = Math.sin(b / 2.0);
        d = 2
                * R
                * Math.asin(Math.sqrt(sa2 * sa2 + Math.cos(lat1)
                * Math.cos(lat2) * sb2 * sb2));
        return d;
    }

    public int compareTo(user_model o) {
        int u1 = this.msisdn;
        long t1 = this.utc_ms;
        int u2 = o.msisdn;
        long t2 = o.utc_ms;
        if(u1 == u2){
            if(t1-t2<0){
                return -1;
            }else if (t1-t2>0) {
                return 1;
            }else {
                return 0;
            }
        }else {
            return u1 - u2;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.msisdn);
        dataOutput.writeLong(this.utc_ms);
        dataOutput.writeInt(this.base_station);
        dataOutput.writeDouble(this.longi);
        dataOutput.writeDouble(this.lati);
        dataOutput.writeInt(this.cluster);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.msisdn = dataInput.readInt();
        this.utc_ms = dataInput.readLong();
        this.base_station = dataInput.readInt();
        this.longi = dataInput.readDouble();
        this.lati = dataInput.readDouble();
        this.cluster = dataInput.readInt();
    }
}
