package org.qcri.rheem.core.optimizer.cardinality;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.configuration.FunctionalKeyValueProvider;
import org.qcri.rheem.core.api.configuration.KeyValueProvider;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.plan.rheemplan.test.TestFilterOperator;
import org.qcri.rheem.core.plan.rheemplan.test.TestJoin;
import org.qcri.rheem.core.plan.rheemplan.test.TestLoopHead;
import org.qcri.rheem.core.plan.rheemplan.test.TestSource;
import org.qcri.rheem.core.util.RheemCollections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link LoopSubplanCardinalityPusher}.
 */
public class LoopSubplanCardinalityPusherTest {

    private Configuration configuration;

    @Before
    public void setUp() {
        this.configuration = mock(Configuration.class);

        KeyValueProvider<OutputSlot<?>, CardinalityEstimator> estimatorProvider =
                new FunctionalKeyValueProvider<>((outputSlot, requestee) -> {
                    assert outputSlot.getOwner().isElementary()
                            : String.format("Cannot provide estimator for composite %s.", outputSlot.getOwner());
                    return ((ElementaryOperator) outputSlot.getOwner())
                            .getCardinalityEstimator(outputSlot.getIndex(), this.configuration)
                            .orElse(null);
                });
        when(this.configuration.getCardinalityEstimatorProvider()).thenReturn(estimatorProvider);
    }

    @Test
    public void testWithSingleLoopAndSingleIteration() {

        TestLoopHead<Integer> loopHead = new TestLoopHead<>(Integer.class);
        loopHead.setNumExpectedIterations(1);

        TestFilterOperator<Integer> inLoopFilter = new TestFilterOperator<>(Integer.class);
        final double filterSelectivity = 0.7d;
        inLoopFilter.setSelectivity(filterSelectivity);
        loopHead.connectTo("loopOutput", inLoopFilter, "input");
        inLoopFilter.connectTo("output", loopHead, "loopInput");

        final LoopSubplan loop = LoopIsolator.isolate(loopHead);
        Assert.assertNotNull(loop);
        OptimizationContext optimizationContext = new OptimizationContext(loop, this.configuration);
        final OptimizationContext.OperatorContext loopCtx = optimizationContext.getOperatorContext(loop);
        final CardinalityEstimate inputCardinality = new CardinalityEstimate(123, 321, 0.123d);
        loopCtx.setInputCardinality(0, inputCardinality);
        loop.propagateInputCardinality(0, loopCtx);

        final CardinalityPusher pusher = new LoopSubplanCardinalityPusher(loop, this.configuration);
        pusher.push(loopCtx, this.configuration);

        final CardinalityEstimate expectedCardinality = new CardinalityEstimate(
                Math.round(inputCardinality.getLowerEstimate() * filterSelectivity),
                Math.round(inputCardinality.getUpperEstimate() * filterSelectivity),
                inputCardinality.getCorrectnessProbability()
        );
        Assert.assertEquals(expectedCardinality, loopCtx.getOutputCardinality(0));

    }

    @Test
    public void testWithSingleLoopAndManyIteration() {

        TestLoopHead<Integer> loopHead = new TestLoopHead<>(Integer.class);
        loopHead.setNumExpectedIterations(1000);

        TestFilterOperator<Integer> inLoopFilter = new TestFilterOperator<>(Integer.class);
        final double filterSelectivity = 0.7d;
        inLoopFilter.setSelectivity(filterSelectivity);
        loopHead.connectTo("loopOutput", inLoopFilter, "input");
        inLoopFilter.connectTo("output", loopHead, "loopInput");

        final LoopSubplan loop = LoopIsolator.isolate(loopHead);
        Assert.assertNotNull(loop);
        OptimizationContext optimizationContext = new OptimizationContext(loop, this.configuration);
        final OptimizationContext.OperatorContext loopCtx = optimizationContext.getOperatorContext(loop);
        final CardinalityEstimate inputCardinality = new CardinalityEstimate(123, 321, 0.123d);
        loopCtx.setInputCardinality(0, inputCardinality);
        loop.propagateInputCardinality(0, loopCtx);

        final CardinalityPusher pusher = new LoopSubplanCardinalityPusher(loop, this.configuration);
        pusher.push(loopCtx, this.configuration);

        final CardinalityEstimate expectedCardinality = new CardinalityEstimate(
                Math.round(inputCardinality.getLowerEstimate() * Math.pow(filterSelectivity, 1000)),
                Math.round(inputCardinality.getUpperEstimate() * Math.pow(filterSelectivity, 1000)),
                inputCardinality.getCorrectnessProbability()
        );
        Assert.assertTrue(expectedCardinality.equalsWithinDelta(loopCtx.getOutputCardinality(0), 0.0001, 1, 1));

    }

    @Test
    public void testWithSingleLoopWithConstantInput() {

        TestSource<Integer> mainSource = new TestSource<>(Integer.class);
        TestSource<Integer> sideSource = new TestSource<>(Integer.class);

        TestLoopHead<Integer> loopHead = new TestLoopHead<>(Integer.class);
        final int numIterations = 3;
        loopHead.setNumExpectedIterations(numIterations);
        mainSource.connectTo("output", loopHead, "initialInput");

        TestJoin<Integer, Integer, Integer> inLoopJoin = new TestJoin<>(Integer.class, Integer.class, Integer.class);
        loopHead.connectTo("loopOutput", inLoopJoin, "input0");
        sideSource.connectTo("output", inLoopJoin, "input1");
        inLoopJoin.connectTo("output", loopHead, "loopInput");

        final LoopSubplan loop = LoopIsolator.isolate(loopHead);
        Assert.assertNotNull(loop);

        OptimizationContext optimizationContext = new OptimizationContext(loop, this.configuration);
        final OptimizationContext.OperatorContext loopCtx = optimizationContext.getOperatorContext(loop);

        final CardinalityEstimate mainInputCardinality = new CardinalityEstimate(123, 321, 0.123d);
        InputSlot<?> mainLoopInput = RheemCollections.getSingle(mainSource.getOutput("output").getOccupiedSlots());
        loopCtx.setInputCardinality(mainLoopInput.getIndex(), mainInputCardinality);
        loop.propagateInputCardinality(mainLoopInput.getIndex(), loopCtx);

        final CardinalityEstimate sideInputCardinality = new CardinalityEstimate(5, 10, 0.9d);
        InputSlot<?> sideLoopInput = RheemCollections.getSingle(sideSource.getOutput("output").getOccupiedSlots());
        loopCtx.setInputCardinality(sideLoopInput.getIndex(), sideInputCardinality);
        loop.propagateInputCardinality(sideLoopInput.getIndex(), loopCtx);

        final CardinalityPusher pusher = new LoopSubplanCardinalityPusher(loop, this.configuration);
        pusher.push(loopCtx, this.configuration);

        final CardinalityEstimate expectedCardinality = new CardinalityEstimate(
                (long) Math.pow(sideInputCardinality.getLowerEstimate(), numIterations) * mainInputCardinality.getLowerEstimate(),
                (long) Math.pow(sideInputCardinality.getUpperEstimate(), numIterations) * mainInputCardinality.getUpperEstimate(),
                Math.min(mainInputCardinality.getCorrectnessProbability(), sideInputCardinality.getCorrectnessProbability())
                        * Math.pow(TestJoin.ESTIMATION_CERTAINTY, numIterations)
        );
        final CardinalityEstimate outputCardinality = loopCtx.getOutputCardinality(0);
        Assert.assertTrue(
                String.format("Expected %s, got %s.", expectedCardinality, outputCardinality),
                expectedCardinality.equalsWithinDelta(outputCardinality, 0.0001, 0, 0));
    }

    @Test
    public void testNestedLoops() {

        TestLoopHead<Integer> outerLoopHead = new TestLoopHead<>(Integer.class);
        outerLoopHead.setNumExpectedIterations(100);

        TestFilterOperator<Integer> inOuterLoopFilter = new TestFilterOperator<>(Integer.class);
        outerLoopHead.connectTo("loopOutput", inOuterLoopFilter, "input");
        inOuterLoopFilter.setSelectivity(0.9d);

        TestLoopHead<Integer> innerLoopHead = new TestLoopHead<>(Integer.class);
        inOuterLoopFilter.connectTo("output", innerLoopHead, "initialInput");
        innerLoopHead.setNumExpectedIterations(100);

        TestFilterOperator<Integer> inInnerLoopFilter = new TestFilterOperator<>(Integer.class);
        innerLoopHead.connectTo("loopOutput", inInnerLoopFilter, "input");
        inInnerLoopFilter.connectTo("output", innerLoopHead, "loopInput");
        innerLoopHead.connectTo("finalOutput", outerLoopHead, "loopInput");
        inInnerLoopFilter.setSelectivity(0.1d);

        LoopSubplan innerLoop = LoopIsolator.isolate(innerLoopHead);
        Assert.assertNotNull(innerLoop);
        LoopSubplan outerLoop = LoopIsolator.isolate(outerLoopHead);
        Assert.assertNotNull(outerLoop);

        OptimizationContext optimizationContext = new OptimizationContext(outerLoop, this.configuration);
        final OptimizationContext.OperatorContext loopCtx = optimizationContext.getOperatorContext(outerLoop);
        final CardinalityEstimate inputCardinality = new CardinalityEstimate(123, 321, 0.123d);
        loopCtx.setInputCardinality(0, inputCardinality);
        outerLoop.propagateInputCardinality(0, loopCtx);

        final CardinalityPusher pusher = new LoopSubplanCardinalityPusher(outerLoop, this.configuration);
        pusher.push(loopCtx, this.configuration);

        double loopSelectivity = Math.pow(
                inOuterLoopFilter.getSelectivity()
                        * Math.pow(inInnerLoopFilter.getSelectivity(), innerLoop.getNumExpectedIterations()),
                outerLoop.getNumExpectedIterations()
        );
        final CardinalityEstimate expectedCardinality = new CardinalityEstimate(
                Math.round(inputCardinality.getLowerEstimate() * loopSelectivity),
                Math.round(inputCardinality.getUpperEstimate() * loopSelectivity),
                inputCardinality.getCorrectnessProbability()
        );
        Assert.assertTrue(expectedCardinality.equalsWithinDelta(loopCtx.getOutputCardinality(0), 0.0001, 1, 1));

    }

}
