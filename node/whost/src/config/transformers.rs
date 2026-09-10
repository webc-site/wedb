use std::marker::PhantomData;

/// libs/host/Configuration/GarnetCustomTransformers.cs:IGarnetCustomTransformer
pub trait IGarnetCustomTransformer<TIn, TOut> {}

/// libs/host/Configuration/GarnetCustomTransformers.cs:FileToContentTransformer
pub struct FileToContentTransformer;

/// libs/host/Configuration/GarnetCustomTransformers.cs:ArrayToFirstItemTransformer
pub struct ArrayToFirstItemTransformer<T>(PhantomData<T>);

/// libs/host/Configuration/GarnetCustomTransformers.cs:NonDefaultObjectToBooleanTransformer
pub struct NonDefaultObjectToBooleanTransformer<T>(PhantomData<T>);
