package scala.objectify.appengine

import com.googlecode.objectify.{annotation, ObjectifyFactory}
import com.googlecode.objectify.impl.translate._
import com.googlecode.objectify.impl.Node
import com.googlecode.objectify.repackaged.gentyref.GenericTypeReflector
import java.lang.reflect.Type

import com.googlecode.objectify.impl.Path
import com.googlecode.objectify.impl.Property
import com.googlecode.objectify.impl.translate.CreateContext
import com.googlecode.objectify.impl.translate.LoadContext
import com.googlecode.objectify.impl.translate.SaveContext
import scala.annotation.meta.field
import scala.collection.mutable
import com.googlecode.objectify.annotation.EmbedMap
import scala.Predef._


object Annotations {

  type Id = annotation.Id@field
  type Parent = annotation.Parent@field
  type Load = annotation.Load@field
  type Embed = annotation.Embed@field
  type EmbedMap = annotation.EmbedMap@field
  type AlsoLoad = annotation.AlsoLoad@field
  type Ignore = annotation.Ignore@field
  type IgnoreLoad = annotation.IgnoreLoad@field
  type IgnoreSave = annotation.IgnoreSave@field
  type Index = annotation.Index@field
  type Mapify = annotation.Mapify@field
  type Serialize = annotation.Serialize@field
  type Translate = annotation.Translate@field
  type Unindex = annotation.Unindex@field
}


object ScalaObjectify {


  def add(fact: ObjectifyFactory) {
    fact.getTranslators.add(new ScalaMapTranslatorFactory())
    fact.getTranslators.add(new ScalaCollectionTranslatorFactory())
  }
}


class ScalaMapTranslatorFactory extends TranslatorFactory[mutable.Map[String, String]] {

  import scala.collection.JavaConversions._


  def create(path: Path, property: Property, `type`: Type, ctx: CreateContext): Translator[mutable.Map[String, String]] = {
    type Z = mutable.Map[_, _]
    val collectionType: Class[Z] = GenericTypeReflector.erase(`type`).asInstanceOf[Class[Z]]

    if (!classOf[mutable.Map[_, _]].isAssignableFrom(collectionType)) null
    else if (GenericTypeReflector.getTypeParameter(`type`, classOf[mutable.Map[AnyRef, AnyRef]].getTypeParameters()(0)) != classOf[String]) null
    else if (property.getAnnotation(classOf[EmbedMap]) == null) throw new IllegalStateException("Map<String, ?> fields must have the @EmbedMap annotation. Check " + property)
    else {

      val fact = ctx.getFactory
      val componentType = GenericTypeReflector.getTypeParameter(`type`, classOf[mutable.Map[AnyRef, AnyRef]].getTypeParameters()(1))
      val componentTranslator = fact.getTranslators.create[String](path, property, componentType, ctx)
      try {
        ctx.enterCollection(path)
        new MapNodeTranslator[mutable.Map[String, String]]() {

          override def loadMap(node: Node, ctx: LoadContext): mutable.Map[String, String] = {
            mutable.Map(node.map(n => n.getPath.getSegment -> n.getPropertyValue.toString).toList: _*)
          }


          override def saveMap(pojo: mutable.Map[String, String], path: Path, index: Boolean, ctx: SaveContext): Node = {
            if (pojo.isEmpty) throw new SkipException()
            val addToList = addToListCtx(componentTranslator, path, index, ctx) _
            pojo.keys.foreach(validateEntry)
            pojo.foldLeft(new Node(path))(addToList)
          }


        }
      } finally {
        ctx.exitCollection()

      }

    }

  }

  private def addToListCtx(componentTranslator: Translator[String], path: Path, index: Boolean, ctx: SaveContext)(node: Node, n: (String, String)) = {
    val child = componentTranslator.save(n._2, path.extend(n._1), index, ctx)
    node.addToMap(child)
    node
  }

  private def validateEntry(key: String) = {
    if (key == null) throw new IllegalArgumentException("Map keys cannot be null")
    if (key.contains(".")) throw new IllegalArgumentException("Map keys cannot contain '.' characters")
  }
}


class ScalaCollectionTranslatorFactory extends TranslatorFactory[Traversable[AnyRef]] {

  import scala.collection.JavaConversions._


  def create(path: Path, property: Property, `type`: Type, ctx: CreateContext): Translator[Traversable[AnyRef]] = {

    type Z = Traversable[_]

    val collectionType: Class[Z] = GenericTypeReflector.erase(`type`).asInstanceOf[Class[Z]]


    if (!classOf[Traversable[_]].isAssignableFrom(collectionType)) null
    else try {
      ctx.enterCollection(path)

      new ListNodeTranslator[Traversable[AnyRef]]() {
        override def loadList(node: Node, ctx: LoadContext): Traversable[AnyRef] = {
          node.map(_.getPropertyValue).to[Set]
        }


        override def saveList(pojo: Traversable[AnyRef], path: Path, index: Boolean, ctx: SaveContext): Node = {
          if (pojo.isEmpty) throw new SkipException()
          pojo.foldLeft(new Node(path))(addToList)
        }

        private def addToList(node: Node, n: AnyRef) = {
          node.addToList(Node.of(n))
          node
        }
      }
    } finally {
      ctx.exitCollection()
    }
  }
}

