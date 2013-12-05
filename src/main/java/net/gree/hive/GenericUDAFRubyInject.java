package net.gree.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.JobConf;
import org.jruby.embed.AttributeName;
import org.jruby.embed.ScriptingContainer;

import java.util.ArrayList;

@Description(name = "rb_inject",
        value = "_FUNC_(method, arg) - " + "Aggregate by using JRuby scriptlet",
        extended = "Examples:\n" +
                "set rb.script = def sum(memo, arg) memo + arg end ; \n" +
                "select user, rb_inject('sum', spent) from log group by user;")
@UDFType(stateful = false)
public class GenericUDAFRubyInject extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(GenericUDAFRubyInject.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        // argument check
        if (parameters.length < 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "At least two arguments are expected.");
        }

        return new GenericUDAFRubyInjectEvaluator();
    }

    public static class GenericUDAFRubyInjectEvaluator extends GenericUDAFEvaluator {
        public static final String PARTIAL_OP = "op";
        public static final String PARTIAL_VAL = "val";

        private String rbEnvScript;
        private JobConf jobConf;
        private String iterateMethod;
        private ObjectInspector retOI;
        private StructObjectInspector partialOI;
        private ObjectInspectorConverters.Converter argsConverter;
        private ObjectInspectorConverters.Converter partialConverter;
        private ScriptingContainer container;
        private Object receiver;

        @Override
        public void configure(MapredContext mapredContext) {
            jobConf = mapredContext.getJobConf();
            rbEnvScript = jobConf.get(RubyUtils.CONF_RB_SCRIPT);
            initializeJRubyRuntime();
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            // in which mode we get raw input
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                if (!(parameters[0] instanceof WritableConstantStringObjectInspector)) {
                    throw new UDFArgumentTypeException(0, "Can't found argument containing ruby method/script");
                }

                // string method name
                iterateMethod = ((WritableConstantStringObjectInspector) parameters[0])
                        .getWritableConstantValue().toString().trim();

                argsConverter = ObjectInspectorConverters.getConverter(
                        parameters[1],
                        RubyUtils.resolveOI(parameters[1])
                );

                if (m == Mode.PARTIAL1) {
                    partialOI = genPartialOI(RubyUtils.resolveOI(parameters[1]));
                    return partialOI;
                } else {
                    // result type is same as resolved parameter type
                    retOI = RubyUtils.resolveOI(parameters[1]);
                    return retOI;
                }

            } else {
                // in which mode we get partial input

                // PARTIAL2 / FINAL
                partialOI = (StructObjectInspector) RubyUtils.resolveOI(parameters[0]);
                partialConverter = ObjectInspectorConverters.getConverter(
                        parameters[0],
                        partialOI
                );

                if (m == Mode.PARTIAL2) {
                    // continue partial
                    return partialOI;
                } else {
                    // return final result
                    return partialOI.getStructFieldRef(PARTIAL_VAL).getFieldObjectInspector();
                }
            }
        }

        private void initializeJRubyRuntime() {
            container = new ScriptingContainer();

            if (jobConf != null) {
                container.getLoadPaths().add(jobConf.get(RubyUtils.CONF_JRB_LOAD_PATH));
            }
            container.setAttribute(AttributeName.SHARING_VARIABLES, false);
            receiver = container.runScriptlet(rbEnvScript);

            LOG.info("initialized JRuby runtime (" +
                    container.getCompatVersion() + ", " +
                    container.getCompileMode()
                    + ")");
        }

        static class ReduceAggregationBuffer implements AggregationBuffer {
            Object container = null;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new ReduceAggregationBuffer();
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((ReduceAggregationBuffer) agg).container = null;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            Object arg = argsConverter.convert(parameters[1]);
            ReduceAggregationBuffer myBuffer = (ReduceAggregationBuffer) agg;
            if (myBuffer.container == null) {
                myBuffer.container = arg;
            } else {
                Object[] args = new Object[2];
                args[0] = myBuffer.container;
                args[1] = arg;
                Object ret = container.callMethod(receiver, iterateMethod, args);
                myBuffer.container = ret;
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            Object[] partial = new Object[2];
            partial[0] = iterateMethod;
            partial[1] = ((ReduceAggregationBuffer) agg).container;
            return partial;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {

            Object data = partialConverter.convert(partial);
            if (iterateMethod == null) {
                iterateMethod = (String) partialOI.getStructFieldData(data, partialOI.getStructFieldRef(PARTIAL_OP));
            }

            Object arg = partialOI.getStructFieldData(data, partialOI.getStructFieldRef(PARTIAL_VAL));

            if (((ReduceAggregationBuffer) agg).container == null) {
                ((ReduceAggregationBuffer) agg).container = arg;
            } else {
                Object[] args = new Object[2];
                args[0] = ((ReduceAggregationBuffer) agg).container;
                args[1] = arg;

                Object ret = container.callMethod(receiver, iterateMethod, args);
                ((ReduceAggregationBuffer) agg).container = ret;
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            return ((ReduceAggregationBuffer) agg).container;
        }

        // partial data type : struct<op:List<String>, partial:__TYPE__ [, ret:__TYPE__]>
        public static StructObjectInspector genPartialOI(ObjectInspector val) {

            ArrayList<String> fnl = new ArrayList<String>(2);
            fnl.add(0, PARTIAL_OP);
            fnl.add(1, PARTIAL_VAL);
            ArrayList<ObjectInspector> foil = new ArrayList<ObjectInspector>(2);
            foil.add(0, PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                    PrimitiveObjectInspector.PrimitiveCategory.STRING));
            foil.add(1, val);

            return ObjectInspectorFactory.getStandardStructObjectInspector(fnl, foil);
        }

    }

}

