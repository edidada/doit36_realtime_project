package cn.doitedu.fin.risk;

final class Numbers {
    private Numbers() { }

    static double finite(double value, String field) {
        if (!Double.isFinite(value)) { throw new IllegalArgumentException(field + " must be finite"); }
        return value;
    }

    static double positive(double value, String field) {
        if (finite(value, field) <= 0) { throw new IllegalArgumentException(field + " must be positive"); }
        return value;
    }

    static double result(double value) {
        if (!Double.isFinite(value)) { throw new ArithmeticException("Numerical result is not finite"); }
        return value;
    }

    static String text(String value, String field) {
        if (value == null || value.isBlank()) { throw new IllegalArgumentException(field + " must not be blank"); }
        return value;
    }
}
