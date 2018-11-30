/*
 * Copyright (C) 2016-2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import akka.actor.DeadLetter
import akka.actor.StashOverflowException
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.StashBuffer
import akka.annotation.InternalApi
import akka.persistence.DiscardToDeadLetterStrategy
import akka.persistence.ReplyToStrategy
import akka.persistence.ThrowOverflowExceptionStrategy
import akka.util.ConstantFun
import akka.util.OptionVal
import akka.{ actor ⇒ a }

/** INTERNAL API: Stash management for persistent behaviors */
@InternalApi
private[akka] trait StashManagement[C, E, S] {
  import akka.actor.typed.scaladsl.adapter._

  def setup: BehaviorSetup[C, E, S]

  private def context: ActorContext[InternalProtocol] = setup.context

  private def stashState: StashState = setup.stashState

  private def internalStashBuffer: StashBuffer[InternalProtocol] = stashState.internalStashBuffer

  protected def isInternalStashEmpty: Boolean = internalStashBuffer.isEmpty

  private def externalStashBuffer: StashBuffer[InternalProtocol] = stashState.externalStashBuffer

  /**
   * Stash a command to the internal stash buffer, which is used while waiting for persist to be completed.
   */
  protected def stashInternal(msg: InternalProtocol): Unit =
    stash(msg, internalStashBuffer)

  /**
   * Stash a command to the external stash buffer, which is used when `Stash` effect is used.
   */
  protected def stashExternal(msg: InternalProtocol): Unit =
    stash(msg, externalStashBuffer)

  private def stash(msg: InternalProtocol, buffer: StashBuffer[InternalProtocol]): Unit = {
    if (setup.settings.logOnStashing) setup.log.debug(
      "Stashing message to {} stash: [{}] ",
      if (buffer eq internalStashBuffer) "internal" else "external", msg)

    try buffer.stash(msg) catch {
      case e: StashOverflowException ⇒
        setup.stashOverflowStrategy match {
          case DiscardToDeadLetterStrategy ⇒
            val noSenderBecauseAkkaTyped: a.ActorRef = a.ActorRef.noSender
            context.system.deadLetters.tell(DeadLetter(msg, noSenderBecauseAkkaTyped, context.self.toUntyped))

          case ReplyToStrategy(_) ⇒
            throw new RuntimeException("ReplyToStrategy does not make sense at all in Akka Typed, since there is no sender()!")

          case ThrowOverflowExceptionStrategy ⇒
            throw e
        }
    }
  }

  /**
   * `tryUnstashOne` is called at the end of processing each command (or when persist is completed)
   */
  protected def tryUnstashOne(behavior: Behavior[InternalProtocol]): Behavior[InternalProtocol] = {
    val buffer =
      if (stashState.isUnstashAllExternalInProgress)
        externalStashBuffer
      else
        internalStashBuffer

    if (buffer.nonEmpty) {
      if (setup.settings.logOnStashing) setup.log.debug(
        "Unstashing message from {} stash: [{}]",
        if (buffer eq internalStashBuffer) "internal" else "external", buffer.head)

      if (stashState.isUnstashAllExternalInProgress)
        stashState.decrementUnstashAllExternalProgress()

      buffer.unstash(setup.context, behavior, 1, ConstantFun.scalaIdentityFunction)
    } else behavior

  }

  /**
   * Subsequent `tryUnstashOne` will first drain the external stash buffer, before using the
   * internal stash buffer. It will unstash as many commands as are in the buffer when
   * `unstashAllExternal` was called, i.e. if subsequent commands stash more those will
   * not be unstashed until `unstashAllExternal` is called again.
   */
  protected def unstashAllExternal(): Unit = {
    if (externalStashBuffer.nonEmpty) {
      if (setup.settings.logOnStashing) setup.log.debug(
        "Unstashing all [{}] messages from external stash, first is: [{}]",
        externalStashBuffer.size, externalStashBuffer.head)
      stashState.startUnstashAllExternal()
      // tryUnstashOne is called from EventSourcedRunning at the end of processing each command (or when persist is completed)
    }
  }

}

/**
 * INTERNAL API
 * Main reason for introduction of this trait is stash buffer reference management
 * in order to survive restart of internal behavior
 */
@InternalApi private[akka] trait StashReferenceManagement {

  private var stashBuffer: OptionVal[StashBuffer[InternalProtocol]] = OptionVal.None

  def stashBuffer(settings: EventSourcedSettings): StashBuffer[InternalProtocol] = {
    val buffer: StashBuffer[InternalProtocol] = stashBuffer match {
      case OptionVal.Some(value) ⇒ value
      case _                     ⇒ StashBuffer(settings.stashCapacity)
    }
    this.stashBuffer = OptionVal.Some(buffer)
    stashBuffer.get
  }

  def clearStashBuffer(): Unit = stashBuffer = OptionVal.None
}

/** INTERNAL API: stash buffer state in order to survive restart of internal behavior */
@InternalApi
private[akka] class StashState(settings: EventSourcedSettings) {

  private var _internalStashBuffer: StashBuffer[InternalProtocol] = StashBuffer(settings.stashCapacity)
  private var _externalStashBuffer: StashBuffer[InternalProtocol] = StashBuffer(settings.stashCapacity)
  private var unstashAllExternalInProgress = 0

  def internalStashBuffer: StashBuffer[InternalProtocol] = _internalStashBuffer

  def externalStashBuffer: StashBuffer[InternalProtocol] = _externalStashBuffer

  def clearStashBuffers(): Unit = {
    _internalStashBuffer = StashBuffer(settings.stashCapacity)
    _externalStashBuffer = StashBuffer(settings.stashCapacity)
    unstashAllExternalInProgress = 0
  }

  def isUnstashAllExternalInProgress: Boolean =
    unstashAllExternalInProgress > 0

  def incrementUnstashAllExternalProgress(): Unit =
    unstashAllExternalInProgress += 1

  def decrementUnstashAllExternalProgress(): Unit =
    unstashAllExternalInProgress -= 1

  def startUnstashAllExternal(): Unit =
    unstashAllExternalInProgress = _externalStashBuffer.size

}
