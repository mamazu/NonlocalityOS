#![feature(map_try_insert)]
use std::collections::BTreeMap;

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Copy, Clone)]
pub struct RegisterId(u16);

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Copy, Clone)]
pub enum RegisterValue {
    Boolean(bool),
    Byte(u8),
}

pub enum Parser {
    IsEndOfInput(RegisterId),
    ReadInputByte(RegisterId),
    Condition(RegisterId, Box<Parser>),
    Fail,
    Sequence(Vec<Parser>),
    Not {
        from: RegisterId,
        to: RegisterId,
    },
    Constant(RegisterId, RegisterValue),
    WriteOutputByte(RegisterId),
    WriteOutputSeparator,
    Loop(RegisterId, Box<Parser>),
    RequireDigit {
        input: RegisterId,
        output: RegisterId,
    },
    Add {
        destination: RegisterId,
        summand: RegisterId,
    },
    Multiply {
        destination: RegisterId,
        factor: RegisterId,
    },
}

pub enum InterpreterStatus {
    WaitingForInput,
    Failed,
    Completed,
    CompletedWithExtraneousInput,
    ErrorInParser,
}

pub trait WriteOutput {
    fn write_byte(&mut self, element: u8);
    fn write_separator(&mut self);
}

struct Interpreter<'t> {
    registers: BTreeMap<RegisterId, RegisterValue>,
    position: Vec<(&'t [Parser], usize)>,
    buffered_input: Option<u8>,
}

impl<'t> Interpreter<'t> {
    pub fn new(parser: &'t Parser) -> Interpreter<'t> {
        Interpreter {
            registers: BTreeMap::new(),
            position: vec![(std::slice::from_ref(parser), 0)],
            buffered_input: None,
        }
    }

    pub fn advance_with_input(
        &mut self,
        input: Option<u8>,
        output: &mut dyn WriteOutput,
    ) -> InterpreterStatus {
        self.buffered_input = input;
        self.advance(output)
    }

    fn advance(&mut self, output: &mut dyn WriteOutput) -> InterpreterStatus {
        loop {
            let position_in_innermost_sequence = match self.position.last_mut() {
                Some(element) => element,
                None => {
                    return if self.buffered_input.is_some() {
                        InterpreterStatus::CompletedWithExtraneousInput
                    } else {
                        InterpreterStatus::Completed
                    }
                }
            };
            assert!(position_in_innermost_sequence.1 <= position_in_innermost_sequence.0.len());
            if position_in_innermost_sequence.1 == position_in_innermost_sequence.0.len() {
                self.position
                    .pop()
                    .expect("As far as we know, the stack shouldn't be empty right now.");
                continue;
            }
            let sequence_element =
                &position_in_innermost_sequence.0[position_in_innermost_sequence.1];
            position_in_innermost_sequence.1 += 1;
            match self.enter_parser(sequence_element, output) {
                Some(status) => return status,
                None => {}
            }
        }
    }

    fn enter_parser(
        &mut self,
        parser: &'t Parser,
        output: &mut dyn WriteOutput,
    ) -> Option<InterpreterStatus> {
        match parser {
            Parser::IsEndOfInput(destination) => self.write_register(
                *destination,
                &RegisterValue::Boolean(self.buffered_input.is_none()),
            ),
            Parser::ReadInputByte(destination) => match self.buffered_input {
                Some(byte) => {
                    self.write_register(*destination, &RegisterValue::Byte(byte));
                    return Some(InterpreterStatus::WaitingForInput);
                }
                None => return Some(InterpreterStatus::Failed),
            },
            Parser::Condition(cause, action) => {
                let register_read_result = self.registers.get(cause);
                match register_read_result {
                    Some(register_value) => match register_value {
                        RegisterValue::Boolean(true) => {
                            self.position.push((std::slice::from_ref(&*action), 0));
                        }
                        RegisterValue::Boolean(false) => {}
                        RegisterValue::Byte(_) => return Some(InterpreterStatus::ErrorInParser),
                    },
                    None => return Some(InterpreterStatus::ErrorInParser),
                }
            }
            Parser::Fail => return Some(InterpreterStatus::Failed),
            Parser::Sequence(inner_sequence) => {
                self.position.push((&inner_sequence[..], 0));
            }
            Parser::Not { from, to } => {
                let register_read_result = self.registers.get(from);
                let result_of_not_operation = match register_read_result {
                    Some(register_value) => match register_value {
                        RegisterValue::Boolean(boolean) => !boolean,
                        RegisterValue::Byte(_) => return Some(InterpreterStatus::ErrorInParser),
                    },
                    None => return Some(InterpreterStatus::ErrorInParser),
                };
                self.write_register(*to, &RegisterValue::Boolean(result_of_not_operation));
            }
            Parser::Constant(destination, value) => {
                self.write_register(*destination, value);
            }
            Parser::WriteOutputByte(from) => {
                let register_read_result = self.registers.get(from);
                match register_read_result {
                    Some(register_value) => match register_value {
                        RegisterValue::Boolean(_) => return Some(InterpreterStatus::ErrorInParser),
                        RegisterValue::Byte(byte) => output.write_byte(*byte),
                    },
                    None => return Some(InterpreterStatus::ErrorInParser),
                }
            }
            Parser::WriteOutputSeparator => output.write_separator(),
            Parser::Loop(condition, body) => {
                let register_read_result = self.registers.get(condition);
                match register_read_result {
                    Some(register_value) => match register_value {
                        RegisterValue::Boolean(true) => {
                            // this is the loop magic:
                            self.position.last_mut().unwrap().1 -= 1;

                            self.position.push((std::slice::from_ref(&*body), 0));
                        }
                        RegisterValue::Boolean(false) => {}
                        RegisterValue::Byte(_) => return Some(InterpreterStatus::ErrorInParser),
                    },
                    None => return Some(InterpreterStatus::ErrorInParser),
                }
            }
            Parser::RequireDigit { input, output } => {
                let register_read_result = self.registers.get(input);
                let digit: u8 = match register_read_result {
                    Some(register_value) => match register_value {
                        RegisterValue::Boolean(_) => return Some(InterpreterStatus::ErrorInParser),
                        RegisterValue::Byte(byte) => match byte {
                            b'0' => 0,
                            b'1' => 1,
                            b'2' => 2,
                            b'3' => 3,
                            b'4' => 4,
                            b'5' => 5,
                            b'6' => 6,
                            b'7' => 7,
                            b'8' => 8,
                            b'9' => 9,
                            _ => return Some(InterpreterStatus::Failed),
                        },
                    },
                    None => return Some(InterpreterStatus::ErrorInParser),
                };
                self.write_register(*output, &RegisterValue::Byte(digit));
            }
            Parser::Add {
                destination,
                summand,
            } => match self.calculate_binary_operation(*destination, *summand, u8::checked_add) {
                Some(status) => return Some(status),
                None => {}
            },
            Parser::Multiply {
                destination,
                factor,
            } => match self.calculate_binary_operation(*destination, *factor, u8::checked_mul) {
                Some(status) => return Some(status),
                None => {}
            },
        }
        None
    }

    fn write_register(&mut self, id: RegisterId, value: &RegisterValue) {
        self.registers.insert(id, *value);
    }

    fn calculate_binary_operation(
        &mut self,
        destination: RegisterId,
        operand: RegisterId,
        operation: fn(u8, u8) -> Option<u8>,
    ) -> Option<InterpreterStatus> {
        let first_operand: u8 = match self.registers.get(&destination) {
            Some(register_value) => match register_value {
                RegisterValue::Boolean(_) => return Some(InterpreterStatus::ErrorInParser),
                RegisterValue::Byte(byte) => *byte,
            },
            None => return Some(InterpreterStatus::ErrorInParser),
        };
        let second_operand: u8 = match self.registers.get(&operand) {
            Some(register_value) => match register_value {
                RegisterValue::Boolean(_) => return Some(InterpreterStatus::ErrorInParser),
                RegisterValue::Byte(byte) => *byte,
            },
            None => return Some(InterpreterStatus::ErrorInParser),
        };
        let result = operation(first_operand, second_operand);
        match result {
            Some(sum) => {
                self.write_register(destination, &RegisterValue::Byte(sum));
            }
            None => return Some(InterpreterStatus::Failed),
        };
        None
    }
}

pub trait ReadInput {
    fn read_input(&mut self) -> Option<u8>;
}

pub struct Slice<'t> {
    remaining: &'t [u8],
}

impl<'t> Slice<'t> {
    fn new(input: &'t str) -> Slice<'t> {
        Slice {
            remaining: input.as_bytes(),
        }
    }
}

impl<'t> ReadInput for Slice<'t> {
    fn read_input(&mut self) -> Option<u8> {
        match self.remaining.split_at_checked(1) {
            Some((head, tail)) => {
                self.remaining = tail;
                Some(head[0])
            }
            None => None,
        }
    }
}

struct Ignorance {}

impl WriteOutput for Ignorance {
    fn write_byte(&mut self, _element: u8) {}

    fn write_separator(&mut self) {}
}

pub fn is_match(parser: &Parser, input: &mut dyn ReadInput) -> Option<bool> {
    let mut interpreter = Interpreter::new(parser);
    loop {
        let next = input.read_input();
        let status = interpreter.advance_with_input(next, &mut Ignorance {});
        match status {
            InterpreterStatus::WaitingForInput => {}
            InterpreterStatus::Failed => return Some(false),
            InterpreterStatus::Completed => return Some(true),
            InterpreterStatus::CompletedWithExtraneousInput => return Some(false),
            InterpreterStatus::ErrorInParser => return None,
        }
    }
}

struct OutputBuffer {
    output: Vec<Option<Vec<u8>>>,
}

impl OutputBuffer {
    fn require_bytes(&mut self) -> &mut Vec<u8> {
        if match self.output.last() {
            Some(last) => last.is_none(),
            None => true,
        } {
            self.output.push(Some(Vec::new()))
        }
        let maybe_last: Option<&mut Option<Vec<u8>>> = self.output.last_mut();
        let definitely_last: &mut Option<Vec<u8>> = maybe_last.unwrap();
        let bytes: &mut Vec<u8> = definitely_last.as_mut().unwrap();
        bytes
    }
}

impl WriteOutput for OutputBuffer {
    fn write_byte(&mut self, element: u8) {
        self.require_bytes().push(element)
    }

    fn write_separator(&mut self) {
        self.output.push(None)
    }
}

pub enum ParseResult {
    Success {
        output: Vec<Option<Vec<u8>>>,
        has_extraneous_input: bool,
    },
    Failed,
    ErrorInParser,
}

pub fn parse(parser: &Parser, input: &mut dyn ReadInput) -> ParseResult {
    let mut interpreter = Interpreter::new(parser);
    let mut output_buffer = OutputBuffer { output: Vec::new() };
    loop {
        let next = input.read_input();
        let status = interpreter.advance_with_input(next, &mut output_buffer);
        match status {
            InterpreterStatus::WaitingForInput => {}
            InterpreterStatus::Failed => return ParseResult::Failed,
            InterpreterStatus::Completed => {
                return ParseResult::Success {
                    output: output_buffer.output,
                    has_extraneous_input: false,
                }
            }
            InterpreterStatus::CompletedWithExtraneousInput => {
                return ParseResult::Success {
                    output: output_buffer.output,
                    has_extraneous_input: true,
                }
            }
            InterpreterStatus::ErrorInParser => return ParseResult::ErrorInParser,
        }
    }
}

#[test]
fn test_empty_parser() {
    let parser = Parser::Sequence(vec![]);
    assert_eq!(Some(true), is_match(&parser, &mut Slice::new("")));
    assert_eq!(Some(false), is_match(&parser, &mut Slice::new("a")));
    assert_eq!(
        Some(false),
        is_match(&parser, &mut Slice::new("aaaaaaaaaaaaaaaaaa"))
    );
}

#[test]
fn test_fail() {
    let parser = Parser::Sequence(vec![
        Parser::IsEndOfInput(RegisterId(0)),
        Parser::Not {
            from: RegisterId(0),
            to: RegisterId(1),
        },
        Parser::Condition(RegisterId(1), Box::new(Parser::Fail)),
    ]);
    assert_eq!(Some(true), is_match(&parser, &mut Slice::new("")));
    assert_eq!(Some(false), is_match(&parser, &mut Slice::new("a")));
    assert_eq!(
        Some(false),
        is_match(&parser, &mut Slice::new("aaaaaaaaaaaaaaaaaa"))
    );
}

#[test]
fn test_extraneous_input() {
    let parser = Parser::Sequence(vec![]);
    let result = parse(&parser, &mut Slice::new("a"));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(0, output.len());
            assert!(has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test]
fn test_read_input_success() {
    let parser = Parser::Sequence(vec![
        Parser::ReadInputByte(RegisterId(0)),
        Parser::WriteOutputByte(RegisterId(0)),
    ]);
    let result = parse(&parser, &mut Slice::new("a"));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(1, output.len());
            {
                let element = &output[0];
                let non_separator = element.as_ref().unwrap();
                assert_eq!(&[b'a'][..], &non_separator[..]);
            }
            assert!(!has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test]
fn test_read_input_failure() {
    let parser = Parser::Sequence(vec![
        Parser::ReadInputByte(RegisterId(0)),
        Parser::WriteOutputByte(RegisterId(0)),
    ]);
    let result = parse(&parser, &mut Slice::new(""));
    match result {
        ParseResult::Success {
            output: _,
            has_extraneous_input: _,
        } => panic!(),
        ParseResult::Failed => {}
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test]
fn test_output_byte() {
    let parser = Parser::Sequence(vec![
        Parser::Constant(RegisterId(0), RegisterValue::Byte(123)),
        Parser::WriteOutputByte(RegisterId(0)),
    ]);
    let result = parse(&parser, &mut Slice::new(""));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(1, output.len());
            {
                let element = &output[0];
                let non_separator = element.as_ref().unwrap();
                assert_eq!(&[123u8][..], &non_separator[..]);
            }
            assert!(!has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test]
fn test_output_bytes() {
    let parser = Parser::Sequence(vec![
        Parser::Constant(RegisterId(0), RegisterValue::Byte(123)),
        Parser::WriteOutputByte(RegisterId(0)),
        Parser::Constant(RegisterId(1), RegisterValue::Byte(76)),
        Parser::WriteOutputByte(RegisterId(1)),
    ]);
    let result = parse(&parser, &mut Slice::new(""));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(1, output.len());
            {
                let element = &output[0];
                let non_separator = element.as_ref().unwrap();
                assert_eq!(&[123u8, 76u8][..], &non_separator[..]);
            }
            assert!(!has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test]
fn test_output_separator() {
    let parser = Parser::Sequence(vec![Parser::WriteOutputSeparator]);
    let result = parse(&parser, &mut Slice::new(""));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(1, output.len());
            {
                let element = &output[0];
                assert!(element.is_none());
            }
            assert!(!has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test]
fn test_mixed_output() {
    let parser = Parser::Sequence(vec![
        Parser::Constant(RegisterId(0), RegisterValue::Byte(123)),
        Parser::WriteOutputByte(RegisterId(0)),
        Parser::WriteOutputSeparator,
        Parser::Constant(RegisterId(1), RegisterValue::Byte(76)),
        Parser::WriteOutputByte(RegisterId(1)),
        Parser::WriteOutputSeparator,
    ]);
    let result = parse(&parser, &mut Slice::new(""));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(4, output.len());
            {
                let element = &output[0];
                let non_separator = element.as_ref().unwrap();
                assert_eq!(&[123u8][..], &non_separator[..]);
            }
            {
                let element = &output[1];
                assert!(element.is_none());
            }
            {
                let element = &output[2];
                let non_separator = element.as_ref().unwrap();
                assert_eq!(&[76u8][..], &non_separator[..]);
            }
            {
                let element = &output[3];
                assert!(element.is_none());
            }
            assert!(!has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}

#[test]
fn test_number_parsing() {
    let accumulator = RegisterId(0);
    let loop_condition = RegisterId(1);
    let constant_10 = RegisterId(2);
    let input_byte = RegisterId(3);
    let input_digit = RegisterId(4);
    let parser = Parser::Sequence(vec![
        Parser::Constant(accumulator, RegisterValue::Byte(0)),
        Parser::Constant(loop_condition, RegisterValue::Boolean(true)),
        Parser::Constant(constant_10, RegisterValue::Byte(10)),
        Parser::Loop(
            loop_condition,
            Box::new(Parser::Sequence(vec![
                Parser::ReadInputByte(input_byte),
                Parser::RequireDigit {
                    input: input_byte,
                    output: input_digit,
                },
                Parser::Multiply {
                    destination: accumulator,
                    factor: constant_10,
                },
                Parser::Add {
                    destination: accumulator,
                    summand: input_digit,
                },
                Parser::IsEndOfInput(loop_condition),
                Parser::Not {
                    from: loop_condition,
                    to: loop_condition,
                },
            ])),
        ),
        Parser::WriteOutputByte(accumulator),
    ]);
    let result = parse(&parser, &mut Slice::new("123"));
    match result {
        ParseResult::Success {
            output,
            has_extraneous_input,
        } => {
            assert_eq!(1, output.len());
            {
                let element = &output[0];
                let non_separator = element.as_ref().unwrap();
                assert_eq!(&[123u8][..], &non_separator[..]);
            }
            assert!(!has_extraneous_input);
        }
        ParseResult::Failed => panic!(),
        ParseResult::ErrorInParser => panic!(),
    }
}