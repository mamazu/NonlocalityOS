#![feature(map_try_insert)]
use std::collections::BTreeMap;

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Copy, Clone)]
pub struct RegisterId(u16);

pub enum Parser {
    IsEndOfInput(RegisterId),
    Condition(RegisterId, Box<Parser>),
    Fail,
    Sequence(Vec<Parser>),
    Not { from: RegisterId, to: RegisterId },
}

pub enum InterpreterStatus {
    WaitingForInput,
    Failed,
    Completed,
    CompletedWithExtraneousInput,
    ErrorInParser,
}

enum RegisterValue {
    Boolean(bool),
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

    pub fn advance_with_input(&mut self, input: Option<u8>) -> InterpreterStatus {
        self.buffered_input = input;
        self.advance()
    }

    fn advance(&mut self) -> InterpreterStatus {
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
            match sequence_element {
                Parser::IsEndOfInput(destination) => {
                    if !self.write_register(
                        *destination,
                        RegisterValue::Boolean(self.buffered_input.is_none()),
                    ) {
                        return InterpreterStatus::ErrorInParser;
                    }
                }
                Parser::Condition(cause, action) => {
                    let register_read_result = self.registers.get(cause);
                    match register_read_result {
                        Some(register_value) => match register_value {
                            RegisterValue::Boolean(true) => {
                                self.position.push((std::slice::from_ref(&*action), 0));
                            }
                            RegisterValue::Boolean(false) => {}
                        },
                        None => return InterpreterStatus::ErrorInParser,
                    }
                }
                Parser::Fail => return InterpreterStatus::Failed,
                Parser::Sequence(inner_sequence) => {
                    self.position.push((&inner_sequence[..], 0));
                }
                Parser::Not { from, to } => {
                    let register_read_result = self.registers.get(from);
                    let result_of_not_operation = match register_read_result {
                        Some(register_value) => match register_value {
                            RegisterValue::Boolean(boolean) => !boolean,
                        },
                        None => return InterpreterStatus::ErrorInParser,
                    };
                    if !self.write_register(*to, RegisterValue::Boolean(result_of_not_operation)) {
                        return InterpreterStatus::ErrorInParser;
                    }
                }
            }
        }
    }

    fn write_register(&mut self, id: RegisterId, value: RegisterValue) -> bool {
        let result = self.registers.try_insert(id, value);
        result.is_ok()
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

pub fn is_match(parser: &Parser, input: &mut dyn ReadInput) -> Option<bool> {
    let mut interpreter = Interpreter::new(parser);
    loop {
        let next = input.read_input();
        let status = interpreter.advance_with_input(next);
        match status {
            InterpreterStatus::WaitingForInput => {}
            InterpreterStatus::Failed => return Some(false),
            InterpreterStatus::Completed => return Some(true),
            InterpreterStatus::CompletedWithExtraneousInput => return Some(false),
            InterpreterStatus::ErrorInParser => return None,
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
