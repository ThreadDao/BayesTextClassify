package exception;

import org.apache.commons.lang.StringUtils;

import java.util.Objects;
import java.util.stream.IntStream;

public class BayesException extends Exception {
    public BayesException() {
    }
    public BayesException(String message) {
        super(message);
    }
    public BayesException(String message,Object... objects) {
        super(formatMessage(message,objects));
    }

    private static String formatMessage(String message,Object[] objects){
        if(StringUtils.isBlank(message)){
            return "";
        }
        StringBuilder messageBuilder = new StringBuilder();
        int[] codePoints = message.codePoints().toArray();
        int length = codePoints.length;
        int paramLength = objects.length;
        int paramIndex = 0;
        Object exception = null;
        for(int i = 0;i<length;i++){
            if(codePoints[i]=='{'&&i<length&&codePoints[i+1]=='}'){
                if(paramIndex<paramLength){
                    messageBuilder.append(String.valueOf(objects[paramIndex++]));
                    i++;
                }else if (objects[paramIndex++] instanceof Throwable){
                    exception = objects[paramIndex-1];
                }
            }else{
                messageBuilder.appendCodePoint(codePoints[i]);
            }
        }
        if(Objects.nonNull(exception)){
            messageBuilder.append("\n");
            messageBuilder.append(((Throwable)exception).getMessage());
        }
        return messageBuilder.toString();
    }

}
