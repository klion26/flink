package org.apache.flink.runtime.state.ttl;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * {@link Matcher Matchers} for {@link TtlValue}.
 */
public class TtlValueMatchers {

	/**
	 * Creates a matcher that matches when the given value and timestamp matchers match the value
	 * and timestamp in a {@link TtlValue};
	 */
	public static <T> Matcher<TtlValue<T>> ttlValue(
			Matcher<T> valueMatcher,
			Matcher<Long> timestampMatcher) {
		return new IsTtlValue<>(valueMatcher, timestampMatcher);
	}

	static class IsTtlValue<T> extends TypeSafeDiagnosingMatcher<TtlValue<T>> {
		private final Matcher<T> valueMatcher;
		private final Matcher<Long> timestampMatcher;

		public IsTtlValue(Matcher<T> valueMatcher, Matcher<Long> timestampMatcher) {
			this.valueMatcher = valueMatcher;
			this.timestampMatcher = timestampMatcher;
		}

		@Override
		protected boolean matchesSafely(TtlValue<T> item, Description mismatchDescription) {
			mismatchDescription.appendText("TtlValue with value ");
			mismatchDescription.appendValue(item.getUserValue());
			mismatchDescription.appendText(" with timestamp ");
			mismatchDescription.appendValue(item.getLastAccessTimestamp());
			return valueMatcher.matches(item.getUserValue()) &&
					timestampMatcher.matches(item.getLastAccessTimestamp());
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("TtlValue with value ");
			valueMatcher.describeTo(description);
			description.appendText(" with timestamp ");
			timestampMatcher.describeTo(description);
		}
	}
}
