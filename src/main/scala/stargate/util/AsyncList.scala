package stargate.util

import java.util.concurrent.{Executor, Executors}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}


class AsyncList[T] private (private val value: Future[Option[(T, () => AsyncList[T])]]) {

  import AsyncList._

  var tailCache: Option[AsyncList[T]] = None

  def isEmpty(executor: ExecutionContext): Future[Boolean] = {
    value.map(_.isEmpty)(executor)
  }

  def maybeHead(executor: ExecutionContext): Future[Option[T]] = {
    value.map(_.map(_._1))(executor)
  }

  def head(executor: ExecutionContext): Future[T] = {
    value.map(_.get._1)(executor)
  }

  def tail(executor: ExecutionContext): AsyncList[T] = {
    this.synchronized {
      if(tailCache.isEmpty) {
        tailCache = Some(unfuture(value.map(_.get._2())(executor), executor))
      }
    }
    tailCache.get
  }

  def map[T2](f: T=>T2, executor: ExecutionContext): AsyncList[T2] = {
    unfuture(isEmpty(executor).map(isEmpty => {
      if(isEmpty) {
        empty[T2]
      } else {
        val newHead = head(executor).map(f)(executor)
        val getTail = () => this.tail(executor).map(f, executor)
        push(newHead, getTail, executor)
      }
    })(executor), executor)
  }


  def filter(f: T=>Boolean, executor: ExecutionContext): AsyncList[T] = {
    unfuture(this.value.map(value => {
      if(value.isEmpty) {
        empty[T]
      } else {
        val head = value.get._1
        val filterTail = () => value.get._2().filter(f, executor)
        if(f(value.get._1)) {
          new AsyncList(Future.successful(Some(head, filterTail)))
        } else {
          filterTail()
        }
      }

    })(executor), executor)
  }

  // note: causes whole list to be read eagerly
  def foldLeft[T2](init: T2)(f: (T2, T) => T2, executor: ExecutionContext): Future[T2] = {
    this.isEmpty(executor).flatMap(isEmpty => {
      if(isEmpty) {
        Future.successful(init)
      } else {
        this.head(executor).flatMap(head => {
          val next = f(init, head)
          this.tail(executor).foldLeft(next)(f, executor)
        })(executor)
      }
    })(executor)
  }

  def flatMap[T2](f: T=>AsyncList[T2], executor: ExecutionContext): AsyncList[T2] = {
    flatten(this.map(f, executor), executor)
  }

  def splitAt(index: Int, executor: ExecutionContext): (Future[List[T]], AsyncList[T]) = {
    val wrapped: Future[(List[T], AsyncList[T])] = if(index <= 0) {
      Future.successful((List.empty, this))
    } else {
      this.isEmpty(executor).flatMap(isEmpty => {
        if (isEmpty) {
          Future.successful((List.empty[T], empty[T]))
        } else {
          implicit val ec: ExecutionContext = executor
          for {
            head <- this.head(executor)
            recurse = this.tail(executor).splitAt(index - 1, executor)
            restOfHead <- recurse._1
          } yield (head +: restOfHead, recurse._2)
        }
      })(executor)
    }
    (wrapped.map(_._1)(executor), unfuture(wrapped.map(_._2)(executor), executor))
  }

  def take(index: Int, executor: ExecutionContext): Future[List[T]] = this.splitAt(index, executor)._1
  def drop(index: Int, executor: ExecutionContext): AsyncList[T] = this.splitAt(index, executor)._2

  def toList(executor: ExecutionContext): Future[List[T]] = {
    this.isEmpty(executor).flatMap(isEmpty => {
      if(isEmpty) {
        Future.successful(List.empty)
      } else {
        implicit val ec: ExecutionContext = executor
        for {
          head <- this.head(executor)
          tail <- this.tail(executor).toList(executor)
        } yield (List(head) ++ tail)
      }
    })(executor)
  }

  def length(executor: ExecutionContext): Future[Int] = {
    this.foldLeft(0)((sum, _) => sum + 1, executor)
  }

  override def toString: String = {
    val threadPool = Executors.newFixedThreadPool(1)
    implicit val ec = ExecutionContext.fromExecutor(threadPool)
    val result = Await.result(this.toList(ec).map("Async" + _.toString), Duration.Inf)
    threadPool.shutdownNow()
    result
  }
}

object AsyncList {

  def empty[T] = new AsyncList[T](Future.successful(None))

  def singleton[T](single: Future[T], executor: ExecutionContext): AsyncList[T] = {
    push(single, ()=>empty[T], executor)
  }
  def singleton[T](single: T): AsyncList[T] = {
    new AsyncList[T](Future.successful(Some((single, ()=>empty))))
  }

  def push[T](head: Future[T], getTail: () => AsyncList[T], executor: ExecutionContext): AsyncList[T] = {
    new AsyncList[T](head.map(Some(_, getTail))(executor))
  }

  def append[T](a: AsyncList[T], b: () => AsyncList[T], executor: ExecutionContext): AsyncList[T] = {
    unfuture(a.isEmpty(executor).map(isEmpty => {
      if(isEmpty) {
        b()
      } else {
        push(a.head(executor), () => append(a.tail(executor), b, executor), executor)
      }
    })(executor), executor)
  }

  def append[T](a: AsyncList[T], b: AsyncList[T], executor: ExecutionContext): AsyncList[T] = {
    append(a, ()=>b, executor)
  }

  def flatten[T](lists: AsyncList[AsyncList[T]], executor: ExecutionContext): AsyncList[T] = {
    unfuture(lists.isEmpty(executor).flatMap(isEmpty => if(isEmpty) {
      Future.successful(empty[T])
    } else {
      lists.head(executor).map(head => append(head, ()=>flatten(lists.tail(executor), executor), executor))(executor)
    })(executor), executor)
  }

  def filterSome[T](list: AsyncList[Option[T]], executor: ExecutionContext): AsyncList[T] = {
    list.filter(_.isDefined, executor).map(_.get, executor)
  }

  def unfuture[T](futureList: Future[AsyncList[T]], executor: ExecutionContext): AsyncList[T] = {
    new AsyncList(futureList.flatMap(_.value)(executor))
  }
  def unfuture[T](futureList: AsyncList[Future[T]], executor: ExecutionContext): AsyncList[T] = {
    new AsyncList(futureList.value.flatMap(value => {
      if(value.isEmpty) {
        Future.successful(None)
      } else {
        value.get._1.map(head => Some((head, () => unfuture(value.get._2(), executor))))(executor)
      }
    })(executor))
  }

  def fromList[T](list: List[T]): AsyncList[T] = {
    if(list.isEmpty) {
      empty[T]
    } else {
      new AsyncList[T](Future.successful(Some((list.head, () => fromList(list.tail)))))
    }
  }
}


