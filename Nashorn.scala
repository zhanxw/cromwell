import javax.script.{ScriptContext, SimpleScriptContext}

import jdk.nashorn.api.scripting.{ClassFilter, NashornScriptEngineFactory}
import wom.util.JsUtil.{ScriptEngineFactory, getNashornClassLoader, nashornStrictArgs, noJavaClassFilter}

object Nashorn extends Application {
  def main(args: Array[String]): Unit {
    val values: java.util.Map[String, AnyRef] = new java.util.Map[String, AnyRef]("inputs" -> "lol")
    val engine = new NashornScriptEngineFactory.getScriptEngine(Array("-doe", "--no-java", "--no-syntax-extensions", "--language=es5"),
      Option(Thread.currentThread.getContextClassLoader).getOrElse(classOf[NashornScriptEngineFactory].getClassLoader), new ClassFilter {
        override def exposeToScripts(unused: String): Boolean = false
      })
    val bindings = engine.createBindings()
    bindings.asInstanceOf[java.util.Map[String, Any]].putAll(values)

    val context = new SimpleScriptContext
    context.setBindings(bindings, ScriptContext.ENGINE_SCOPE)
    engine.eval(expr, context)
  }
}