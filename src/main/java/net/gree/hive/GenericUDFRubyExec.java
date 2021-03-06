package net.gree.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.jruby.embed.AttributeName;
import org.jruby.embed.EmbedEvalUnit;
import org.jruby.embed.ScriptingContainer;
import org.jruby.javasupport.JavaEmbedUtils;
import org.jruby.runtime.builtin.IRubyObject;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
/*
rb_exec("&method", args)
rb_exec("script", args)
rb_exec(Map('k','y'), "&method", args)
*/

@Description(name = "rb_exec",
        value = "_FUNC_([ret_type_hint, ]script, arg1, arg2 ...) - " + "Evaluate using JRuby scriptlet",
        extended = "Examples:\n" +
                "select rb_exec('@arg1.to_s + \":\" + @arg2 ', foo, bar) from pokes;")
@UDFType(stateful = false)
public class GenericUDFRubyExec extends GenericUDF {
    private ObjectInspector returnOI;
    transient private ObjectInspectorConverters.Converter[] argsConverters;
    private Log LOG = LogFactory.getLog(GenericUDFRubyExec.class.getName());

    private final Text retText = new Text();

    public static final String CONF_RB_SCRIPT = "rb.script";
    public static final String CONF_JRB_LOAD_PATH = "jruby.load_path";

    enum MODE {METHOD, EVAL}

    public static final String MODE_METHOD_MARK = "&";

    private MODE mode;
    private int scriptParamPos;
    private String evaluateMethod;
    private JobConf jobConf;
    private String rbScriptParam;
    private String rbEnvScript;
    private HiveConf hiveConf;


    transient private ScriptingContainer container;
    private Object receiver;
    transient private EmbedEvalUnit evalUnit;

    // to get value of rb.script & jruby.load_path from Context
    @Override
    public void configure(MapredContext mapredContext) {
        jobConf = mapredContext.getJobConf();
        rbEnvScript = jobConf.get(CONF_RB_SCRIPT);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] parameters) throws UDFArgumentException {

        // argument check
        if (parameters.length < 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "At least one argument is expected.");
        }

        SessionState sessionState = SessionState.get();
        if (sessionState != null) {
            hiveConf = sessionState.getConf();
            rbEnvScript = hiveConf.get(CONF_RB_SCRIPT);
            LOG.info("got rb.script " + rbEnvScript);
        }


        scriptParamPos = 0;
        if (parameters[0] instanceof WritableConstantStringObjectInspector) {
            // no hint of return type found
            returnOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                    PrimitiveObjectInspector.PrimitiveCategory.STRING);
        } else if (parameters.length >= 2 && parameters[1] instanceof WritableConstantStringObjectInspector) {
            scriptParamPos = 1;
            // 1st param is hint of return type
            returnOI = RubyScriptingUtils.resolveOI(parameters[0]);
        } else {
            throw new UDFArgumentTypeException(0, "Couldn't find argument containing ruby method/script");
        }

        rbScriptParam = RubyScriptingUtils.getScriptParam(parameters[scriptParamPos]);

        argsConverters = new ObjectInspectorConverters.Converter[parameters.length - scriptParamPos - 1];
        for (int i = 0; i < argsConverters.length; i++) {
            argsConverters[i] = ObjectInspectorConverters.getConverter(
                    parameters[i + scriptParamPos + 1],
                    RubyScriptingUtils.resolveOI(parameters[i + scriptParamPos + 1])
            );
        }

        initializeJRubyRuntime();

        StringBuilder sb = new StringBuilder();
        sb.append("initialized rb_exec. return type is ");
        sb.append(returnOI.getTypeName());
        if (mode == MODE.METHOD) {
            sb.append(", try to evaluate using method '");
            sb.append(evaluateMethod);
            sb.append("' defined in rb.script as:\n");
            sb.append(rbEnvScript);
        } else {
            sb.append(", try to evaluate using this scriptlet: ");
            sb.append(rbScriptParam);
        }
        LOG.info(sb.toString());

        return returnOI;
    }

    // initialize JRuby runtime here
    // for JDK7+, there may be performance improvement
    // by passing -Djruby.compile.invokedynamic=true to mapred.child.java.opts
    private void initializeJRubyRuntime() {
        container = new ScriptingContainer(org.jruby.embed.LocalContextScope.SINGLETHREAD);

        if (hiveConf != null) {
            container.getLoadPaths().add(hiveConf.get(CONF_JRB_LOAD_PATH));
        }

        if (rbScriptParam.startsWith(MODE_METHOD_MARK)) {
            mode = MODE.METHOD;
            evaluateMethod = rbScriptParam.substring(1);
            container.setAttribute(AttributeName.SHARING_VARIABLES, false);
            receiver = container.runScriptlet(rbEnvScript);
        } else {
            mode = MODE.EVAL;
            evalUnit = container.parse(rbScriptParam);
        }
        LOG.info("initialized JRuby runtime (" +
                container.getCompatVersion() + ", " +
                container.getCompileMode()
                + ")");

    }

    @Override
    public Object evaluate(DeferredObject[] parameters) throws HiveException {
        Object[] args = new Object[argsConverters.length];
        for (int i = 0; i < args.length; i++) {
            args[i] = argsConverters[i].convert(parameters[i + scriptParamPos + 1].get());
        }

        Object ret;
        if (mode == MODE.EVAL) {
            for (int i = 0; i < args.length; i++) {
                container.put("@arg" + (i + 1), args[i]);
            }
            ret = JavaEmbedUtils.rubyToJava((IRubyObject) (evalUnit.run()));
        } else {
            ret = container.callMethod(receiver, evaluateMethod, args);
        }

        if (scriptParamPos == 0) {
            // should return String
            if (ret != null) {
                retText.set(ret.toString());
                return retText;
            } else {
                return null;
            }
        } else {
            return ret;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("rb_exec(");
        if (children.length > 0) {
            sb.append(children[0]);
            for (int i = 1; i < children.length; i++) {
                sb.append(", ");
                sb.append(children[i]);
            }
        }
        sb.append(")");
        return sb.toString();
    }
}

