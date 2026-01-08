package main


import "fmt"




// Minimal operation (exact)
//
// A function that fails the first K times, then succeeds.
// Retry loop with a max attempts limit.
// Question: What is the being run ? -> operation
// Question: An operation can be attempted multiple times ? What should I call an attempt ? -> Let's call it operationAttempt

/*

LOGIC:
An operationAttempt needs to run K times if failing. or if succeeds.
An operation Needs to run some task (some function)
An operationAttempt should have an attemptNumber

*/

const (
	SUCCESS = iota
	FAILED
)

type RetryPolicy struct {
	maxAttempts int
}

func createRetryPolicy(maxAttempts int) RetryPolicy {
	return RetryPolicy{ maxAttempts: maxAttempts }
}


type Operation struct {
	opFunc func() error
	name string
	retryPolicy RetryPolicy
}


func createOperation(name string, retryPolicy RetryPolicy, opFunc func() error) Operation {
	return Operation {
		opFunc: opFunc,
		name: name,
		retryPolicy: retryPolicy,
	}
}


type OperationAttempt struct {
	op Operation
	attemptNum int
}

type OperationResultStatus int

type OperationResult struct {
	opAttempt OperationAttempt
	status OperationResultStatus
	err error
}


func createOperationResult(
	opAttempt OperationAttempt,
	opStatus OperationResultStatus,
	err error,
) OperationResult {
	return OperationResult {
		opAttempt: opAttempt,
		status: opStatus,
		err: err,
	}
}

func runOperation(opAttempt OperationAttempt) OperationResult {
	opFunc := opAttempt.op.opFunc
	err := opFunc()

	if err != nil {
		return createOperationResult(opAttempt, FAILED, err)
	}

	return createOperationResult(opAttempt, SUCCESS, nil)
}


func runOperations(op Operation) error {
	attemptNum := 1

	for attemptNum <= op.retryPolicy.maxAttempts {
		operationAttempt := OperationAttempt {
			op: op,
			attemptNum: attemptNum,
		}
		opResult := runOperation(operationAttempt)
		attemptNum = opResult.opAttempt.attemptNum + 1

		switch opResult.status {
		case FAILED:
		case SUCCESS:
			return nil
		}
	}

		return fmt.Errorf("failed to succeed in %d times\n", op.retryPolicy.maxAttempts)
}


func main() {
	retryPolicy := createRetryPolicy(3)
	op1Func := func() error {
		fmt.Println("HELLO, WORLD")
		return fmt.Errorf("FAILED for unknown reason")
	}
	op1 := createOperation("op1", retryPolicy, op1Func)
	err := runOperations(op1)
	if err != nil {
		fmt.Printf("Error recorded : %v\n\n", err)
	}
}
