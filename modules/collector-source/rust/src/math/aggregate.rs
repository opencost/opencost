use std::marker::PhantomData;

pub trait Aggregate {
    type Item;

    fn append(&mut self, value: Self::Item);
    fn get(&mut self) -> Self::Item;
}

pub struct Average {
    intermediate: (f64, usize),
}

impl Average {
    pub fn new() -> Self {
        Self {
            intermediate: (0.0f64, 0usize),
        }
    }
}

impl Aggregate for Average {
    type Item = f64;

    fn append(&mut self, value: Self::Item) {
        self.intermediate = (self.intermediate.0 + value, self.intermediate.1 + 1);
    }

    fn get(&mut self) -> Self::Item {
        self.intermediate.0 / (self.intermediate.1 as f64)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_thing() {
        //let b: Box<dyn Aggregate> = Box::new(Average::new());
    }
}
/*
pub struct Aggregate<T, U, F, X>
where
    T: Copy,
    U: Copy,
    F: Fn(U, T) -> U,
    X: Fn(U) -> T,
{
    current: U,
    transform: F,
    finalizer: X,
    _marker: PhantomData<T>,
}

impl<T, U, F, X> Aggregate<T, U, F, X>
where
    T: Copy,
    U: Copy,
    F: Fn(U, T) -> U,
    X: Fn(U) -> T,
{
    pub fn new(initial_value: U, transform: F, finalizer: X) -> Self {
        Self {
            current: initial_value,
            transform,
            finalizer,
            _marker: PhantomData,
        }
    }

    pub fn append(&mut self, value: T) {
        self.current = (self.transform)(self.current, value);
    }

    pub fn get(&self) -> T {
        (self.finalizer)(self.current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate() -> anyhow::Result<()> {
        let mut avg_agg = Aggregate::new(
            (0.0, 0u64),
            |curr, value| {
                let mut items = curr.1;
                let mut expanded = curr.0 * items as f64;

                expanded += value;
                items += 1;

                (expanded / (items as f64), items)
            },
            |curr| curr.0,
        );

        let mut total = 0.0;

        for i in 1..=50 {
            let i_f = i as f64;
            total += i_f;

            avg_agg.append(i_f);
        }

        let result = avg_agg.get();
        println!("Value: {}", result);

        let avg = total / 50.0f64;
        println!("Expected: {}", avg);

        Ok(())
    }
}
 */
