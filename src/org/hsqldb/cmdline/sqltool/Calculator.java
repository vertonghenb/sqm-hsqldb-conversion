package org.hsqldb.cmdline.sqltool;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.EnumSet;
public class Calculator {
    private List<Atom> atoms = new ArrayList<Atom>();
    private static Pattern intPattern = Pattern.compile("[+-]?\\d+");
    private Map<String, String> vars;
    private enum MathOp {
        LPAREN('('),
        RPAREN(')'),
        ADD('+'),
        SUBTRACT('-'),
        MULTIPLY('*'),
        DIVIDE('/'),
        REM('%'),
        POWER('^')
        ;
        MathOp(char c) { this.c = c; }
        private char c;
        public String toString() { return Character.toString(c); }
        public static MathOp valueOf(char c) {
            for (MathOp o : MathOp.values())
                if (o.c == c) return o;
            return null;
        }
    }
    private EnumSet<MathOp> TradOrLParen =
            EnumSet.of(MathOp.ADD, MathOp.SUBTRACT, MathOp.LPAREN,
            MathOp.MULTIPLY, MathOp.DIVIDE, MathOp.REM, MathOp.POWER);
    private long deref(String varName) {
        if (!vars.containsKey(varName))
            throw new IllegalStateException("Undefined variable: " + varName);
        try {
            return Long.parseLong(vars.get(varName));
        } catch (NumberFormatException nfe) {
            throw new IllegalStateException(
                    "Variable's value not an integer: " + varName);
        }
    }
    private class Atom {
        private Atom(String token) {
            if (token == null)
                throw new IllegalArgumentException("Tokens may not be null");
            if (token.length() < 1)
                throw new IllegalArgumentException("Tokens may not be empty");
            if (intPattern.matcher(token).matches()) {
                val = Long.parseLong(token);
                return;
            }
            if (token.length() == 1) {
                op = MathOp.valueOf(token.charAt(0));
                if (op != null) return;
            }
            val = deref(token);
        }
        private Atom(MathOp op) { this.op = op; }
        private Atom(long val) { this.val = val; }
        public MathOp op;
        public long val;
        public String toString() {
            return (op == null) ? Long.toString(val) : op.toString();
        }
    }
    public String toString() {
        return atoms.toString();
    }
    public Calculator(String[] sa, Map<String, String> vars) {
        if (vars.size() < 1)
            throw new IllegalArgumentException("No expression supplied");
        this.vars = vars;
        Atom atom = null, prePrevAtom;
        int prevIndex;
        NEXT_TOKEN:
        for (String token : sa) try {
            atom = new Atom(token);
            prevIndex = atoms.size() - 1;
            if (prevIndex < 0) continue;
            if (atoms.get(prevIndex).op != MathOp.SUBTRACT) continue;
            prePrevAtom = (prevIndex > 0) ? atoms.get(prevIndex-1) : null;
            if (prePrevAtom != null && !TradOrLParen.contains(prePrevAtom.op))
                continue;
            if (atom.op == null) {
                atoms.remove(prevIndex);
                atom.val *= -1;
            } else if (atom.op == MathOp.LPAREN) {
                atoms.remove(prevIndex);
                atoms.add(new Atom(-1L));
                atoms.add(new Atom(MathOp.MULTIPLY));
            }
        } finally {
            atoms.add(atom);
        }
    }
    public Calculator(String s, Map<String, String> vars) {
        this(s.replaceAll("([-()*/+^])", " $1 ")
                .trim().split("\\s+"), vars);
    }
    public long reduce(int startAtomIndex, boolean stopAtParenClose) {
        int i;
        Long prevValue = null;
        Atom atom;
        i = startAtomIndex - 1;
        PAREN_SEEKER:
        while (atoms.size() >= ++i) {
            if (atoms.size() == i) {
                if (stopAtParenClose)
                    throw new IllegalStateException(
                            "Unbalanced '" + MathOp.LPAREN + "'");
                break;
            }
            atom = atoms.get(i);
            if (atom.op != null) switch (atom.op) {
              case RPAREN:
                if (!stopAtParenClose)
                    throw new IllegalStateException(
                            "Unbalanced '" + MathOp.RPAREN + "'");
                atoms.remove(i);
                break PAREN_SEEKER;
              case LPAREN: 
                atoms.remove(i);
                atoms.add(i, new Atom(reduce(i, true)));
                break;
              default:
            }
        }
        int remaining = i - startAtomIndex;
        if (remaining < 1)
            throw new IllegalStateException("Empty expression");
        Atom nextAtom;
        MathOp op;
        i = startAtomIndex;
        atom = atoms.get(i);
        if (atom.op != null)
            throw new IllegalStateException(
                    "Expected initial value expected but got operation "
                    + atom.op);
        while (startAtomIndex + remaining > i + 1) {
            if (startAtomIndex + remaining < i + 3)
                throw new IllegalStateException(
                        "No operator/operand pairing remaining");
            nextAtom = atoms.get(i + 1);
            if (nextAtom.op == null)
                throw new IllegalStateException(
                        "Operator expected but got value " + nextAtom.val);
            op = nextAtom.op;
            nextAtom = atoms.get(i + 2);
            if (nextAtom.op != null)
                throw new IllegalStateException(
                        "Value expected but got operator " + nextAtom.op);
            if (op != MathOp.POWER) {
                i += 2;
                atom = nextAtom;
                continue;
            }
            remaining -= 2;
            atoms.remove(i + 1);
            atoms.remove(i + 1);
            long origVal = atom.val;
            atom.val = 1;
            for (int j = 0; j < nextAtom.val; j++) atom.val *= origVal;
        }
        i = startAtomIndex;
        atom = atoms.get(i);
        if (atom.op != null)
            throw new IllegalStateException(
                    "Expected initial value expected but got operation "
                    + atom.op);
        while (startAtomIndex + remaining > i + 1) {
            if (startAtomIndex + remaining < i + 3)
                throw new IllegalStateException(
                        "No operator/operand pairing remaining");
            nextAtom = atoms.get(i + 1);
            if (nextAtom.op == null)
                throw new IllegalStateException(
                        "Operator expected but got value " + nextAtom.val);
            op = nextAtom.op;
            nextAtom = atoms.get(i + 2);
            if (nextAtom.op != null)
                throw new IllegalStateException(
                        "Value expected but got operator " + nextAtom.op);
            if (op != MathOp.MULTIPLY && op != MathOp.DIVIDE && op != MathOp.REM) {
                i += 2;
                atom = nextAtom;
                continue;
            }
            remaining -= 2;
            atoms.remove(i + 1);
            atoms.remove(i + 1);
            if (op == MathOp.MULTIPLY) atom.val *= nextAtom.val;
            else if (op == MathOp.DIVIDE) atom.val /= nextAtom.val;
            else atom.val %= nextAtom.val;
        }
        atom = atoms.remove(startAtomIndex);
        remaining--;
        if (atom.op != null)
            throw new IllegalStateException(
                    "Value expected but got operation " + atom.op);
        long total = atom.val;
        while (remaining > 0) {
            --remaining;
            atom = atoms.remove(startAtomIndex);
            op = atom.op;
            if (op == null)
                throw new IllegalStateException(
                        "Operator expected but got value " + atom.val);
            if (remaining <= 0)
                throw new IllegalStateException("No operand for operator " + op);
            --remaining;
            atom = atoms.remove(startAtomIndex);
            if (atom.op != null)
                throw new IllegalStateException(
                        "Value expected but got operation " + atom.op);
            switch (op) {
              case ADD:
                total += atom.val;
                break;
              case SUBTRACT:
                total -= atom.val;
                break;
              default:
                throw new IllegalStateException("Unknown operator: " + op);
            }
        }
        return total;
    }
    public static void main(String[] sa) {
        if (sa.length != 1)
            throw new IllegalArgumentException(
                    "SYNTAX: java Calculator 'expression'");
        Map<String, String> uV = new HashMap<String, String>();
        uV.put("one", "1");
        uV.put("two", "2");
        uV.put("three", "3");
        uV.put("four", "4");
        uV.put("five", "5");
        uV.put("six", "6");
        uV.put("seven", "7");
        uV.put("eight", "8");
        uV.put("nine", "9");
        Calculator calc = new Calculator(sa[0], uV);
        System.out.println(calc);
        System.out.println(calc.reduce(0, false));
    }
    public static long reassignValue(String assignee,
            Map<String, String> valMap, String opStr, String expr) {
        long outVal = 0;
        try {
            outVal = Long.parseLong(valMap.get(assignee));
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException(
                    "Can not perform a self-operation on a non-integer: "
                    + assignee);
        }
        Long rhValObj = (expr == null || expr.trim().length() < 1) ? null
                : Long.valueOf(
                        new Calculator(expr, valMap).reduce(0, false));
        if (opStr.equals("++")) {
            if (rhValObj != null)
                throw new IllegalStateException(
                        "++ operator takes no right hand operand");
            return ++outVal;
        }
        if (opStr.equals("--")) {
            if (rhValObj != null)
                throw new IllegalStateException(
                        "++ operator takes no right hand operand");
            return --outVal;
        }
        if (rhValObj == null)
            throw new IllegalStateException(
                    "Operator requires a right hand operand: " + opStr);
        long rhVal = rhValObj.intValue();
        if (opStr.equals("+=")) {
            outVal += rhVal;
        } else if (opStr.equals("-=")) {
            outVal -= rhVal;
        } else if (opStr.equals("*=")) {
            outVal *= rhVal;
        } else if (opStr.equals("/=")) {
            outVal /= rhVal;
        } else if (opStr.equals("%=")) {
            outVal %= rhVal;
        } else {
            throw new IllegalStateException("Unsupported operator: " + opStr);
        }
        return outVal;
    }
}