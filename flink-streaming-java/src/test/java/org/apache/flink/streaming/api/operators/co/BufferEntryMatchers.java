package org.apache.flink.streaming.api.operators.co;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * {@link Matcher Matchers} for {@link IntervalJoinOperator.BufferEntry}.
 */
public class BufferEntryMatchers {

	/**
	 * Creates a matcher that matches when the given element and {@code hasBeenJoined} value match
	 * the given matchers.
	 */
	public static <T> Matcher<IntervalJoinOperator.BufferEntry<T>> bufferEntry(
			Matcher<T> elementMatcher,
			Matcher<Boolean> hasBeenJoinedMatcher) {
		return new IsBufferEntry<>(elementMatcher, hasBeenJoinedMatcher);
	}

	static class IsBufferEntry<T> extends TypeSafeDiagnosingMatcher<IntervalJoinOperator.BufferEntry<T>> {
		private final Matcher<T> elementMatcher;
		private final Matcher<Boolean> hasBeenJoinedMatcher;

		public IsBufferEntry(Matcher<T> elementMatcher, Matcher<Boolean> hasBeenJoinedMatcher) {
			this.elementMatcher = elementMatcher;
			this.hasBeenJoinedMatcher = hasBeenJoinedMatcher;
		}

		@Override
		protected boolean matchesSafely(
				IntervalJoinOperator.BufferEntry<T> item, Description mismatchDescription) {
			mismatchDescription.appendText("BufferEntry with element ");
			mismatchDescription.appendValue(item.getElement());
			mismatchDescription.appendText(" with hasBeenJoined ");
			mismatchDescription.appendValue(item.hasBeenJoined());

			return elementMatcher.matches(item.getElement()) &&
					hasBeenJoinedMatcher.matches(item.hasBeenJoined());
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("BufferEntry with element ");
			elementMatcher.describeTo(description);
			description.appendText(" with hasBeenJoined ");
			hasBeenJoinedMatcher.describeTo(description);
		}
	}
}
