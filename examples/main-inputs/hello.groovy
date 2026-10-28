import com.test.bigdata.rtmk.demo9.groovy.HelloInterface

class Greeting implements HelloInterface {
    void sayHello(String name) {
        println "你好，${name}"
    }
}
