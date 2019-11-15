package com.tom.order;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * ClassName: OrderBean
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/12 19:48
 */
public class OrderBean implements Writable {
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(oid);
        out.writeInt(pid);
        out.writeInt(count);
        out.writeUTF(pname);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.oid = in.readInt();
        this.pid = in.readInt();
        this.count = in.readInt();
        this.pname = in.readUTF();
        this.flag = in.readUTF();
    }

    private int oid;
    private int pid;
    private int count;
    private String pname;
    private String flag;

    public OrderBean() {

    }

    @Override
    public String toString() {
        return oid+"\t"+pname+"\t"+count;
    }

    public int getOid() {
        return oid;
    }

    public void setOid(int oid) {
        this.oid = oid;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
}
