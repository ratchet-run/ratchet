package jakarta.validation;

public class ConstraintViolationException extends ValidationException {

  public ConstraintViolationException(String message) {
    super(message);
  }
}
