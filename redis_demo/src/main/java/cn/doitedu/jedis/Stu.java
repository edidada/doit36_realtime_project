package cn.doitedu.jedis;


import java.io.Serializable;

public class Stu implements Serializable {

    public Stu(int id, String name, String phone, boolean niuBi, double salary) {
        this.id = id;
        this.name = name;
        this.phone = phone;
        this.niuBi = niuBi;
        this.salary = salary;
    }

    public int id;
    public String name;
    public String phone;
    public boolean niuBi;
    public double salary;

    @Override
    public String toString() {
        return "Stu{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", phone='" + phone + '\'' +
                ", niuBi=" + niuBi +
                ", salary=" + salary +
                '}';
    }
}
