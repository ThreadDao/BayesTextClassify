package task;

import org.apache.commons.lang.StringUtils;

import java.util.Objects;
import java.util.Properties;

public interface Task {

    String getProfile();

    void setProfile(String profile);

    String readProperty(String propertyKey);

    void process(String[] args) throws Exception ;

}
