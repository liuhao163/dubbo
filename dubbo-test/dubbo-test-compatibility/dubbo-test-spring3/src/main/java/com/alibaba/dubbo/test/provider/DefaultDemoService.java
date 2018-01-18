package com.alibaba.dubbo.test.provider;

import com.alibaba.dubbo.config.annotation.Service;
import com.alibaba.dubbo.demo.DemoService;

/**
 * Default {@link DemoService} implementation
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since 2.5.8
 */
@Service(
        version = "2.5.8",
        application = "dubbo-annotation-provider",
        protocol = "dubbo",
        registry = "my-registry"
)
public class DefaultDemoService implements DemoService {

    @Override
    public String sayHello(String name) {
        return "DefaultDemoService - sayHell() : " + name;
    }

    @Override
    public void sayHelloWorld(String param1,String param2) {
        System.out.println( "DefaultDemoService - sayHell() : " +param1+" "+param2);
    }
}
