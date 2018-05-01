package com.grms.step8;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public class Results  implements WritableComparable<Results> , DBWritable{
    private String uid ;
    private String gid ;
    private int exp ;

    public Results(){

    }
    public Results(String uid,String gid,int exp){
        this.uid = uid ;
        this.gid = gid ;
        this.exp = exp ;

    }
    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getUid() {
        return uid;
    }

    public String getGid() {
        return gid;
    }

    public void setGid(String gid) {
        this.gid = gid;
    }

    public int getExp() {
        return exp;
    }

    public void setExp(int exp) {
        this.exp = exp;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.uid);
        dataOutput.writeUTF(this.gid);
        dataOutput.writeInt(this.exp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uid = dataInput.readUTF();
        this.gid = dataInput.readUTF();
        this.exp = dataInput.readInt();
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {
        ps.setString(1,uid);
        ps.setString(2,gid);
        ps.setInt(3,exp);
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        uid=rs.getString(1);
        gid=rs.getString(2);
        exp=rs.getInt(3);
    }

    @Override
    public int compareTo(Results o) {
        int uidComp=this.uid.compareTo(o.uid);
        int gidComp=this.gid.compareTo(o.gid);
        int indexComp=this.exp-o.exp;
        return uidComp==0?(gidComp==0?indexComp:gidComp):uidComp;
    }
    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof Results)) return false;
        Results that=(Results) o;
        return getExp()==that.getExp()&& Objects.equals(getUid(),that.getUid())&&Objects.equals(getGid(),that.getGid());
    }
    @Override
    public int hashCode(){
        return Objects.hash(getUid(),getGid(),getExp());
    }
    @Override
    public String toString(){
        return "ResultDB{"+"uid='"+uid+'\''+", gid='"+gid+'\''+", exp="+exp+'}';
    }
}
