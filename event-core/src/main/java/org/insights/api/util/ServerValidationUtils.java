package org.insights.api.util;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.Errors;

public class ServerValidationUtils {
	
	protected final static Logger logger = LoggerFactory.getLogger(ServerValidationUtils.class);

	public static void rejectIfNullOrEmpty(Errors errors, String data, String field, String errorMsg) {
		if (data == null || StringUtils.isBlank(data)) {
			errors.rejectValue(field, errorMsg);
		}
	}
	public static void rejectIfAnyException(Errors errors, String errorCode, Exception exception) {
			errors.rejectValue(errorCode, exception.toString());
	}
	public static void rejectIfNull(Errors errors, Object data, String field, String errorMsg) {
		if (data == null) {
			errors.rejectValue(field, errorMsg);
		}
	}

	public static void rejectIfNull(Errors errors, Object data, String field, String errorCode, String errorMsg) {
		if (data == null) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}
	
	public static void rejectIfAlReadyExist(Errors errors, Object data,  String errorCode, String errorMsg) {
		if (data != null) {
			errors.reject(errorCode, errorMsg);
		}
	}
	
	public static void rejectIfNullOrEmpty(Errors errors, Set<?> data, String field, String errorMsg) {
		if (data == null || data.size() == 0) {
			errors.rejectValue(field, errorMsg);
		}
	}

	public static void rejectIfNullOrEmpty(Errors errors, String data, String field, String errorCode, String errorMsg) {
		if (data == null || StringUtils.isBlank(data)) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}

	public static void rejectIfInvalidType(Errors errors, String data, String field, String errorCode, String errorMsg, Map<String, String> typeParam) {
		if (!typeParam.containsKey(data)) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}
	
	public static void rejectIfInvalidDate(Errors errors, Date data, String field, String errorCode, String errorMsg) {
		Date date =new Date();
		if (data.compareTo(date) <= 0) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}

	public static void rejectIfInvalid(Errors errors, Double data, String field, String errorCode, String errorMsg, Map<Double, String> ratingScore) {
		if (!ratingScore.containsKey(data)) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}
	
	public static void logErrorIfZeroLongValue(Boolean isValidEvent, Long data, String field, String errorCode, String eventJson, String errorMsg) {
		if (isValidEvent) {
			if (data == null || data == 0L) {
				isValidEvent = false;
				logger.error(errorMsg + " ErrorCode :" + errorCode + " FieldName :" + field + " : " + eventJson);
			}
		}
	}
	
	public static void logErrorIfNullOrEmpty(Boolean isValidEvent, String data, String field, String errorCode, String eventJson, String errorMsg) {
		if (isValidEvent) {
			if (data == null || StringUtils.isBlank(data) || data.equalsIgnoreCase("null")) {
				isValidEvent = false;
				logger.error(errorMsg + " ErrorCode :" + errorCode + " FieldName :" + field + " : " + eventJson);
			}
		}
	}
	
	public static void logErrorIfNull(Boolean isValidEvent, Object data, String field, String errorMsg) {
		if (isValidEvent) {
			if (data == null) {
				isValidEvent = false;
				logger.error(errorMsg + " : FieldName :" + field + " : ");
			}
		}
	}
}
